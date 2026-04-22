# genie_middleware.py

import streamlit as st
import hashlib
import logging
from datetime import datetime
from db_service import run_warehouse_non_query,run_warehouse_df

logger = logging.getLogger(__name__)
from config import Config

WH = Config.WAREHOUSE_SCHEMA 
# -------------------------------
# Context Management
# -------------------------------
def set_log_context(**kwargs):
    if "genie_log_context" not in st.session_state:
        st.session_state.genie_log_context = {}
    st.session_state.genie_log_context.update(kwargs)


def get_log_context():
    return st.session_state.get("genie_log_context", {})


# -------------------------------
# Utils
# -------------------------------
def _sql_escape(val):
    if val is None:
        return ""
    return str(val).replace("'", "''")


def generate_context_hash(question: str, user: str):
    raw = f"{user}:{question.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()

# -------------------------------
# Middleware Logger (Insert and Update Frequency)
# -------------------------------
def log_event(event_type: str, payload: dict):
    try:
        ctx = get_log_context()

        question = payload.get("question") or ctx.get("question", "")
        session_id = payload.get("session_id") or ctx.get("session_id", "unknown")
        user = payload.get("user") or ctx.get("user", "UNKNOWN")

        context_hash = generate_context_hash(question, user)

        sql_query = _sql_escape(payload.get("sql", ""))
        summary = _sql_escape(payload.get("summary", ""))
        full = _sql_escape(payload.get("full_answer", ""))
        tables = _sql_escape(payload.get("tables", ""))
        filters = _sql_escape(payload.get("filters", ""))
        details = _sql_escape(payload.get("details", ""))
        cache_key = _sql_escape(payload.get("cache_key", ""))

        relevance = payload.get("relevance", 0.0)

        user_esc = _sql_escape(user)
        question_esc = _sql_escape(question)

        #get existing frequency
        existing_frequency = get_existing_question_frequency(question_esc, user_esc)
        new_frequency = existing_frequency + 1

        sql = f"""
        INSERT INTO [{WH}].[{Config.GENIE_CONTEXT_MEMORY_TABLE}] (
            SessionId,
            Username,
            user_id,
            Question,
            AnswerSummary,
            FullAnswer,
            Context_Hash,
            Sql_Query,
            Tables_Used,
            Filters_Applied,
            Relevance_Score,
            Usage_Count,
            Last_Accessed_At,
            CacheKey,
            Frequency,
            Action_Type,
            Action_Details,
            ChatDate,
            CreatedAt,
            UpdatedAt
        )
        VALUES (
            '{session_id}',
            '{user_esc}',
            '{user_esc}',
            '{question_esc}',
            '{summary}',
            '{full}',
            '{context_hash}',
            '{sql_query}',
            '{tables}',
            '{filters}',
            {relevance},
            1,
            GETDATE(),
            '{cache_key}',
            {new_frequency},
            '{event_type}',
            '{details}',
            CAST(GETDATE() AS DATE),
            GETDATE(),
            GETDATE()
        );
        """

        run_warehouse_non_query(sql)

        
        update_frequency = f"""
        UPDATE [{WH}].[{Config.GENIE_CONTEXT_MEMORY_TABLE}]
        SET
            Frequency = {new_frequency},
            Last_Accessed_At = GETDATE(),
            UpdatedAt = GETDATE()
        WHERE [Question] = '{question}'
        AND Username = '{user_esc}';
        """

        run_warehouse_non_query(update_frequency)

    except Exception as e:
        logger.warning(f"[Middleware] Logging failed: {e}")


def get_existing_question_frequency(question: str, user: str) -> int:
    try:
        sql = f"""
        SELECT ISNULL(MAX(Frequency), 0) AS maxFrequency
        FROM [{WH}].[{Config.GENIE_CONTEXT_MEMORY_TABLE}]
        WHERE Question = '{question}'
          AND Username = '{user}'
        """

        result = run_warehouse_df(sql)
        print(f"Frequency query result:\n{result}")

        # ✅ Proper DataFrame emptiness check
        if result is None or result.empty:
            return 0

        # ✅ Safe value extraction
        max_freq = result.iloc[0]["maxFrequency"]
        print(f"Existing frequency result: {max_freq}")

        return int(max_freq) if max_freq is not None else 0

    except Exception as e:
        logger.warning(f"[Middleware] Fetch existing frequency failed: {e}")
        return 0


# -------------------------------
# Middleware Logger (MERGE)
# -------------------------------
def log_events_upsert(event_type: str, payload: dict):
    try:
        ctx = get_log_context()

        question = payload.get("question") or ctx.get("question", "")
        session_id = payload.get("session_id") or ctx.get("session_id", "unknown")
        user = payload.get("user") or ctx.get("user", "UNKNOWN")

        context_hash = generate_context_hash(question, user)

        sql_query = _sql_escape(payload.get("sql", ""))
        summary = _sql_escape(payload.get("summary", ""))
        full = _sql_escape(payload.get("full_answer", ""))
        tables = _sql_escape(payload.get("tables", ""))
        filters = _sql_escape(payload.get("filters", ""))
        details = _sql_escape(payload.get("details", ""))
        cache_key = _sql_escape(payload.get("cache_key", ""))

        relevance = payload.get("relevance", 0.0)

        user_esc = _sql_escape(user)
        question_esc = _sql_escape(question)

        sql = f"""
        MERGE [{WH}].[{Config.GENIE_CONTEXT_MEMORY_TABLE}] AS target
        USING (
            SELECT
                '{session_id}' AS SessionId,
                '{user_esc}' AS Username,
                '{user_esc}' AS user_id,
                '{question_esc}' AS Question,
                '{context_hash}' AS Context_Hash
        ) AS source
        ON target.Context_Hash = source.Context_Hash
           AND target.Username = source.Username

        WHEN MATCHED THEN
            UPDATE SET
                Frequency = target.Frequency + 1,
                Last_Accessed_At = GETDATE(),
                UpdatedAt = GETDATE(),
                AnswerSummary = '{summary}',
                FullAnswer = '{full}',
                Sql_Query = '{sql_query}',
                Tables_Used = '{tables}',
                Filters_Applied = '{filters}',
                Relevance_Score = {relevance},
                CacheKey = '{cache_key}',
                Action_Type = '{event_type}',
                Action_Details = '{details}',
                Usage_Count = ISNULL(target.Usage_Count, 0) + 1

        WHEN NOT MATCHED THEN
            INSERT (
                SessionId, Username, user_id, Question,
                AnswerSummary, FullAnswer, Context_Hash,
                Sql_Query, Tables_Used, Filters_Applied,
                Relevance_Score, Usage_Count, Last_Accessed_At,
                CacheKey, Frequency, Action_Type, Action_Details,
                ChatDate, CreatedAt, UpdatedAt
            )
            VALUES (
                '{session_id}', '{user_esc}', '{user_esc}', '{question_esc}',
                '{summary}', '{full}', '{context_hash}',
                '{sql_query}', '{tables}', '{filters}',
                {relevance}, 1, GETDATE(),
                '{cache_key}', 1, '{event_type}', '{details}',
                CAST(GETDATE() AS DATE), GETDATE(), GETDATE()
            );
        """

        run_warehouse_non_query(sql)

    except Exception as e:
        logger.warning(f"[Middleware] Logging failed: {e}")