"""
Structured logging configuration for the ingestion pipeline.
"""

import os
import logging
import logging.handlers
import structlog
from typing import Any, Dict
from datetime import datetime


class IngestionLogger:
    """Configures structured logging for the ingestion pipeline."""
    
    @staticmethod
    def setup_logging(
        log_level: str = None,
        log_format: str = None,
        log_file: str = None
    ) -> None:
        """Set up structured logging for the application."""
        
        # Get configuration from environment or defaults
        log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
        log_format = log_format or os.getenv('LOG_FORMAT', 'json')
        log_file = log_file or os.getenv('LOG_FILE')
        
        # Configure standard logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(message)s',  # Structlog will handle formatting
            handlers=[]
        )
        
        # Set up handlers
        handlers = []
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper()))
        handlers.append(console_handler)
        
        # File handler (if specified)
        if log_file:
            # Create log directory if it doesn't exist
            log_dir = os.path.dirname(log_file)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            
            # Rotating file handler
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5
            )
            file_handler.setLevel(getattr(logging, log_level.upper()))
            handlers.append(file_handler)
        
        # Configure structlog
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                IngestionLogger._add_correlation_id,
                structlog.processors.UnicodeDecoder(),
                IngestionLogger._get_json_processor(log_format),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        
        # Apply handlers to root logger
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.handlers.extend(handlers)
        
        # Log initialization
        logger = structlog.get_logger()
        logger.info(
            "Logging initialized",
            log_level=log_level,
            log_format=log_format,
            log_file=log_file or "console"
        )
    
    @staticmethod
    def _add_correlation_id(logger, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Add correlation ID to log events if available."""
        # Try to get correlation ID from context
        correlation_id = event_dict.get('correlation_id')
        
        if correlation_id:
            event_dict['correlation_id'] = correlation_id
        
        return event_dict
    
    @staticmethod
    def _get_json_processor(log_format: str):
        """Get the appropriate processor based on format."""
        if log_format.lower() == 'json':
            return structlog.processors.JSONRenderer()
        else:
            return structlog.dev.ConsoleRenderer(colors=True)


class CorrelationLogger:
    """Logger with correlation ID support for tracing operations."""
    
    def __init__(self, correlation_id: str = None):
        self.correlation_id = correlation_id
        self.logger = structlog.get_logger()
        
        if correlation_id:
            self.logger = self.logger.bind(correlation_id=correlation_id)
    
    def info(self, message: str, **kwargs):
        """Log info message with correlation context."""
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with correlation context."""
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with correlation context."""
        self.logger.error(message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with correlation context."""
        self.logger.debug(message, **kwargs)
    
    def exception(self, message: str, **kwargs):
        """Log exception with correlation context."""
        self.logger.exception(message, **kwargs)


class OperationLogger:
    """Context manager for logging operation lifecycle."""
    
    def __init__(self, operation_name: str, correlation_id: str = None, **context):
        self.operation_name = operation_name
        self.correlation_id = correlation_id
        self.context = context
        self.start_time = None
        self.logger = CorrelationLogger(correlation_id)
    
    def __enter__(self):
        """Enter operation context."""
        self.start_time = datetime.now()
        
        self.logger.info(
            f"Operation started: {self.operation_name}",
            operation=self.operation_name,
            **self.context
        )
        
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit operation context."""
        duration = (datetime.now() - self.start_time).total_seconds()
        
        if exc_type is None:
            self.logger.info(
                f"Operation completed: {self.operation_name}",
                operation=self.operation_name,
                duration_seconds=duration,
                **self.context
            )
        else:
            self.logger.error(
                f"Operation failed: {self.operation_name}",
                operation=self.operation_name,
                duration_seconds=duration,
                error_type=exc_type.__name__ if exc_type else None,
                error_message=str(exc_val) if exc_val else None,
                **self.context
            )
        
        return False  # Don't suppress exceptions


def get_logger(correlation_id: str = None) -> CorrelationLogger:
    """Get a logger with optional correlation ID."""
    return CorrelationLogger(correlation_id)


def log_operation(operation_name: str, correlation_id: str = None, **context):
    """Decorator for logging operations."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with OperationLogger(operation_name, correlation_id, **context) as logger:
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    logger.exception(f"Exception in {operation_name}")
                    raise
        return wrapper
    return decorator


# Initialize logging when module is imported
IngestionLogger.setup_logging()
