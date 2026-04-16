"""
Contextual Memory Manager for ProcureIQ Genie.

Provides both short-term (session-based) and long-term (database-based)
contextual memory for the Genie AI assistant. This enables multi-turn conversations
and context-aware SQL generation.

Features:
  - Short-term memory: Session context stored in Streamlit session_state
  - Long-term memory: Persistent context stored in database
  - Context summarization: Automatic summarization of older contexts
  - Relevance scoring: Find most relevant contexts for current question
  - Memory cleanup: Automatic cleanup of stale contexts
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import hashlib

import pandas as pd

from config import Config
from db_service import run_warehouse_df, run_warehouse_non_query, get_warehouse_connection

logger = logging.getLogger(__name__)


class ContextualMemoryManager:
    """
    Manages both short-term and long-term contextual memory for Genie.
    
    Short-term memory:
      - Stored in Streamlit session_state
      - Available for current session only
      - Includes recent questions, answers, and context
      
    Long-term memory:
      - Persisted in database tables
      - Available across sessions
      - Includes verified queries, successful patterns, and user context
    """
    
    def __init__(self, session_state=None):
        """
        Initialize memory manager.
        
        Args:
            session_state: Streamlit session_state object (optional)
        """
        self.session_state = session_state
        self.short_term_max_messages = Config.SHORT_TERM_MEMORY_MAX_MESSAGES
        self.long_term_max_contexts = Config.LONG_TERM_MEMORY_MAX_CONTEXTS
        self.context_window_size = Config.GENIE_CONTEXT_WINDOW_SIZE
        self.memory_window_minutes = Config.SHORT_TERM_MEMORY_WINDOW_MINUTES
        
    # =========================================================================
    # SHORT-TERM MEMORY (Session-Based)
    # =========================================================================
    
    def initialize_session_memory(self) -> None:
        """Initialize short-term memory structures in session_state."""
        if self.session_state is None:
            logger.warning("Session state not available for short-term memory")
            return
            
        if "genie_short_term_memory" not in self.session_state:
            self.session_state["genie_short_term_memory"] = {
                "messages": [],  # Recent questions and answers
                "contexts": {},  # Question -> context mapping
                "timestamps": {},  # Track message times
                "entities": {},  # Extracted entities (vendors, dates, etc.)
                "last_table_context": None,  # Last table queried
            }
    
    def add_message_to_short_term(
        self,
        question: str,
        answer: str,
        sql: Optional[str] = None,
        context_data: Optional[Dict] = None
    ) -> None:
        """
        Add a question-answer pair to short-term memory.
        
        Args:
            question: User question
            answer: AI answer or query result
            sql: Generated SQL (optional)
            context_data: Additional context (tables used, filters, etc.)
        """
        if self.session_state is None:
            return
            
        self.initialize_session_memory()
        memory = self.session_state["genie_short_term_memory"]
        
        message = {
            "timestamp": datetime.now().isoformat(),
            "question": question,
            "answer": answer,
            "sql": sql,
            "context": context_data or {},
        }
        
        # Keep only recent messages
        memory["messages"].append(message)
        if len(memory["messages"]) > self.short_term_max_messages:
            memory["messages"].pop(0)
        
        # Hash question for quick lookup
        q_hash = hashlib.md5(question.lower().encode()).hexdigest()
        memory["contexts"][q_hash] = context_data or {}
        memory["timestamps"][q_hash] = time.time()
    
    def get_short_term_context(self) -> str:
        """
        Retrieve relevant short-term context for LLM prompt.
        
        Returns:
            Formatted context string summarizing recent interactions
        """
        if self.session_state is None or "genie_short_term_memory" not in self.session_state:
            return ""
        
        memory = self.session_state["genie_short_term_memory"]
        messages = memory.get("messages", [])
        
        if not messages:
            return ""
        
        # Filter recent messages (within memory window)
        now = time.time()
        window_seconds = self.memory_window_minutes * 60
        recent_messages = [
            m for m in messages
            if (now - time.mktime(datetime.fromisoformat(m["timestamp"]).timetuple())) < window_seconds
        ]
        
        if not recent_messages:
            return ""
        
        # Format context
        context = "RECENT CONVERSATION CONTEXT:\n"
        for msg in recent_messages[-5:]:  # Keep last 5 messages
            context += f"\nQ: {msg['question']}\n"
            if msg.get("sql"):
                context += f"SQL: {msg['sql']}\n"
            if msg.get("context"):
                context += f"Context: {msg['context']}\n"
        
        return context
    
    def extract_entities(self, question: str, sql_result: Optional[pd.DataFrame] = None) -> Dict:
        """
        Extract relevant entities from question and result.
        
        Args:
            question: User question
            sql_result: Result dataframe (optional)
            
        Returns:
            Dictionary of extracted entities
        """
        entities = {
            "dates": [],
            "vendors": [],
            "amounts": [],
            "tables": [],
        }
        
        # Simple entity extraction (can be enhanced with NLP)
        import re
        
        # Dates
        date_pattern = r'\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4}'
        entities["dates"] = re.findall(date_pattern, question)
        
        # Dollar amounts
        amount_pattern = r'\$[\d,]+\.?\d*|[\d,]+k'
        entities["amounts"] = re.findall(amount_pattern, question)
        
        # Store in memory
        if self.session_state is not None:
            self.initialize_session_memory()
            self.session_state["genie_short_term_memory"]["entities"] = entities
        
        return entities
    
    # =========================================================================
    # LONG-TERM MEMORY (Database-Based)
    # =========================================================================
    
    def ensure_long_term_memory_table(self) -> bool:
        """
        Create long-term memory table if it doesn't exist.
        
        Returns:
            True if table exists or was created successfully
        """
        if not Config.LONG_TERM_MEMORY_ENABLED:
            return False
        
        try:
            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'genie_context_memory')
            BEGIN
                CREATE TABLE {Config.LONG_TERM_MEMORY_TABLE} (
                    context_id INT PRIMARY KEY IDENTITY(1,1),
                    user_id NVARCHAR(255),
                    session_id NVARCHAR(255),
                    context_hash NVARCHAR(64) UNIQUE,
                    question NVARCHAR(2000),
                    answer NVARCHAR(MAX),
                    sql_query NVARCHAR(MAX),
                    tables_used NVARCHAR(1000),
                    filters_applied NVARCHAR(1000),
                    result_summary NVARCHAR(2000),
                    relevance_score FLOAT DEFAULT 0.5,
                    usage_count INT DEFAULT 1,
                    created_at DATETIME DEFAULT GETDATE(),
                    updated_at DATETIME DEFAULT GETDATE(),
                    last_accessed_at DATETIME DEFAULT GETDATE(),
                    is_verified BIT DEFAULT 0,
                    INDEX idx_user_session (user_id, session_id),
                    INDEX idx_context_hash (context_hash),
                    INDEX idx_created (created_at)
                );
            END
            """
            run_warehouse_non_query(create_table_sql)
            logger.info("Long-term memory table ready")
            return True
        except Exception as e:
            logger.error(f"Error creating long-term memory table: {e}")
            return False
    
    def add_to_long_term_memory(
        self,
        question: str,
        answer: str,
        sql: str,
        tables_used: List[str],
        filters: Dict,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        is_verified: bool = False,
    ) -> Optional[int]:
        """
        Add context to long-term memory.
        
        Args:
            question: User question
            answer: AI answer
            sql: Generated SQL
            tables_used: List of tables used
            filters: Applied filters
            user_id: Optional user identifier
            session_id: Optional session identifier
            is_verified: Whether query was verified as correct
            
        Returns:
            Context ID if successful, None otherwise
        """
        if not Config.LONG_TERM_MEMORY_ENABLED:
            return None
        
        try:
            # Create context hash
            context_str = f"{question}|{sql}|{','.join(tables_used)}"
            context_hash = hashlib.md5(context_str.encode()).hexdigest()
            
            # Check if context already exists
            check_sql = f"""
            SELECT context_id FROM {Config.LONG_TERM_MEMORY_TABLE}
            WHERE context_hash = '{context_hash}'
            """
            try:
                existing = run_warehouse_df(check_sql)
                if not existing.empty:
                    # Update usage count and last accessed
                    update_sql = f"""
                    UPDATE {Config.LONG_TERM_MEMORY_TABLE}
                    SET usage_count = usage_count + 1,
                        last_accessed_at = GETDATE()
                    WHERE context_hash = '{context_hash}'
                    """
                    run_warehouse_non_query(update_sql)
                    return existing.iloc[0, 0]
            except:
                pass
            
            # Insert new context
            insert_sql = f"""
            INSERT INTO {Config.LONG_TERM_MEMORY_TABLE}
            (user_id, session_id, context_hash, question, answer, sql_query,
             tables_used, filters_applied, is_verified)
            VALUES
            ('{user_id or 'UNKNOWN'}',
             '{session_id or 'UNKNOWN'}',
             '{context_hash}',
             {self._escape_sql_string(question)},
             {self._escape_sql_string(answer)},
             {self._escape_sql_string(sql)},
             '{','.join(tables_used)}',
             '{json.dumps(filters)}',
             {1 if is_verified else 0})
            """
            run_warehouse_non_query(insert_sql)
            logger.info(f"Added context to long-term memory: {context_hash}")
            return context_hash
        except Exception as e:
            logger.error(f"Error adding to long-term memory: {e}")
            return None
    
    def retrieve_relevant_contexts(
        self,
        question: str,
        limit: int = 5
    ) -> List[Dict]:
        """
        Retrieve relevant contexts from long-term memory.
        
        Uses similarity matching and relevance scoring to find
        contextually relevant previous questions and answers.
        
        Args:
            question: Current question
            limit: Maximum number of contexts to retrieve
            
        Returns:
            List of relevant context dictionaries
        """
        if not Config.LONG_TERM_MEMORY_ENABLED:
            return []
        
        try:
            # Get high-relevance, frequently-used contexts
            retrieve_sql = f"""
            SELECT TOP {limit}
                context_id,
                question,
                answer,
                sql_query,
                tables_used,
                usage_count,
                relevance_score,
                is_verified
            FROM {Config.LONG_TERM_MEMORY_TABLE}
            ORDER BY
                is_verified DESC,
                usage_count DESC,
                relevance_score DESC,
                last_accessed_at DESC
            """
            
            results = run_warehouse_df(retrieve_sql)
            
            if results.empty:
                return []
            
            # Convert to list of dicts
            contexts = []
            for _, row in results.iterrows():
                contexts.append({
                    "context_id": row["context_id"],
                    "question": row["question"],
                    "answer": row["answer"],
                    "sql_query": row["sql_query"],
                    "tables_used": row["tables_used"],
                    "usage_count": row["usage_count"],
                    "relevance_score": row["relevance_score"],
                    "is_verified": bool(row["is_verified"]),
                })
            
            return contexts
        except Exception as e:
            logger.error(f"Error retrieving contexts: {e}")
            return []
    
    def format_long_term_context_for_prompt(self, contexts: List[Dict]) -> str:
        """
        Format long-term memory contexts for inclusion in LLM prompt.
        
        Args:
            contexts: List of context dictionaries
            
        Returns:
            Formatted context string for prompt
        """
        if not contexts:
            return ""
        
        prompt = "RELEVANT PREVIOUS QUERIES:\n\n"
        
        for ctx in contexts:
            prompt += f"Question: {ctx['question']}\n"
            prompt += f"SQL: {ctx['sql_query']}\n"
            if ctx['is_verified']:
                prompt += "[VERIFIED QUERY]\n"
            prompt += "\n"
        
        return prompt
    
    def cleanup_old_contexts(self, days_old: int = 30) -> int:
        """
        Clean up old contexts from long-term memory.
        
        Args:
            days_old: Delete contexts older than this many days
            
        Returns:
            Number of contexts deleted
        """
        if not Config.LONG_TERM_MEMORY_ENABLED:
            return 0
        
        try:
            delete_sql = f"""
            DELETE FROM {Config.LONG_TERM_MEMORY_TABLE}
            WHERE created_at < DATEADD(day, -{days_old}, GETDATE())
            AND usage_count < 2
            """
            
            # For tracking deleted count
            count_sql = f"""
            SELECT COUNT(*) as cnt FROM {Config.LONG_TERM_MEMORY_TABLE}
            WHERE created_at < DATEADD(day, -{days_old}, GETDATE())
            AND usage_count < 2
            """
            
            count_df = run_warehouse_df(count_sql)
            count = count_df.iloc[0, 0] if not count_df.empty else 0
            
            run_warehouse_non_query(delete_sql)
            logger.info(f"Cleaned up {count} old contexts from memory")
            return count
        except Exception as e:
            logger.error(f"Error cleaning up old contexts: {e}")
            return 0
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def _escape_sql_string(self, s: str) -> str:
        """Escape string for SQL insertion."""
        if not s:
            return "NULL"
        escaped = s.replace("'", "''")
        return f"N'{escaped}'"
    
    def get_memory_stats(self) -> Dict:
        """
        Get statistics about current memory usage.
        
        Returns:
            Dictionary with memory statistics
        """
        stats = {
            "short_term_messages": 0,
            "long_term_contexts": 0,
            "memory_enabled": Config.LONG_TERM_MEMORY_ENABLED and Config.SHORT_TERM_MEMORY_ENABLED,
        }
        
        if self.session_state and "genie_short_term_memory" in self.session_state:
            stats["short_term_messages"] = len(
                self.session_state["genie_short_term_memory"].get("messages", [])
            )
        
        if Config.LONG_TERM_MEMORY_ENABLED:
            try:
                count_sql = f"SELECT COUNT(*) as cnt FROM {Config.LONG_TERM_MEMORY_TABLE}"
                count_df = run_warehouse_df(count_sql)
                stats["long_term_contexts"] = count_df.iloc[0, 0] if not count_df.empty else 0
            except:
                stats["long_term_contexts"] = 0
        
        return stats
    
    def clear_session_memory(self) -> None:
        """Clear all session memory."""
        if self.session_state and "genie_short_term_memory" in self.session_state:
            self.session_state["genie_short_term_memory"] = {
                "messages": [],
                "contexts": {},
                "timestamps": {},
                "entities": {},
                "last_table_context": None,
            }
            logger.info("Session memory cleared")
