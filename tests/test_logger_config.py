"""
Tests for src/utils/logger_config.py — setup_pipeline_logger()

NOTE: The `hasHandlers()` check in setup_pipeline_logger traverses the
logger hierarchy. When other modules (DataProfiler, etc.) are imported,
their module-level logger calls add handlers to the root. This means
the factory short-circuits for NEW names. These tests account for that
by testing both the fresh-setup path and the idempotency path.
"""
import logging
from src.utils.logger_config import setup_pipeline_logger


class TestSetupPipelineLogger:
    """Tests for the logger factory function."""

    def test_returns_logger_instance(self, tmp_path):
        """setup_pipeline_logger() should return a logging.Logger."""
        log_file = tmp_path / "test.log"
        logger = setup_pipeline_logger(name="test_returns_inst", log_file=str(log_file))
        assert isinstance(logger, logging.Logger)

    def test_known_logger_has_handlers(self):
        """
        A logger name that was already set up at import time (e.g. 'Profiler')
        should have at least one handler.
        """
        logger = logging.getLogger("Profiler")
        assert logger.hasHandlers()

    def test_no_duplicate_handlers_on_repeated_calls(self, tmp_path):
        """
        Calling setup_pipeline_logger twice with the same name must NOT
        add extra handlers.
        """
        log_file = tmp_path / "test.log"
        # First: clear any prior state so the first call takes the full path
        name = "test_dup_guard"
        existing = logging.getLogger(name)
        existing.handlers.clear()

        logger1 = setup_pipeline_logger(name=name, log_file=str(log_file))
        count_after_first = len(logger1.handlers)

        logger2 = setup_pipeline_logger(name=name, log_file=str(log_file))
        count_after_second = len(logger2.handlers)

        assert count_after_second == count_after_first
        assert logger1 is logger2

    def test_fresh_logger_gets_debug_level_and_handlers(self, tmp_path):
        """
        A freshly created logger (no existing handlers at any level of
        the hierarchy) should be set to DEBUG with two handlers.
        """
        # Temporarily detach root handlers so hasHandlers() is False
        # for our fresh logger name.
        root = logging.getLogger()
        root_handlers = root.handlers[:]
        root.handlers.clear()

        name = "test_fresh_setup"
        fresh = logging.getLogger(name)
        fresh.handlers.clear()
        fresh.setLevel(logging.NOTSET)

        try:
            log_file = tmp_path / "test.log"
            logger = setup_pipeline_logger(name=name, log_file=str(log_file))

            assert logger.level == logging.DEBUG
            handler_types = [type(h) for h in logger.handlers]
            assert logging.FileHandler in handler_types
            assert logging.StreamHandler in handler_types
        finally:
            # Restore root handlers
            root.handlers = root_handlers
