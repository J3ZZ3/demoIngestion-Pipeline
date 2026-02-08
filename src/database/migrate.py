"""
Database migration runner for the ingestion pipeline.
"""

import os
import logging
from pathlib import Path
from src.database.connection import db_manager

logger = logging.getLogger(__name__)


class MigrationManager:
    """Manages database migrations."""
    
    def __init__(self):
        self.migrations_dir = Path(__file__).parent.parent.parent / "database" / "migrations"
    
    def run_migration(self, migration_file: str) -> None:
        """Run a single migration file."""
        migration_path = self.migrations_dir / migration_file
        
        if not migration_path.exists():
            raise FileNotFoundError(f"Migration file not found: {migration_path}")
        
        logger.info(f"Running migration: {migration_file}")
        
        with open(migration_path, 'r', encoding='utf-8') as f:
            migration_sql = f.read()
        
        try:
            # Split migration into individual statements
            statements = [stmt.strip() for stmt in migration_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    db_manager.execute_query(statement, fetch=False)
            
            logger.info(f"Migration completed successfully: {migration_file}")
            
        except Exception as e:
            logger.error(f"Migration failed: {migration_file} - {e}")
            raise
    
    def run_all_migrations(self) -> None:
        """Run all migration files in order."""
        if not self.migrations_dir.exists():
            logger.warning(f"Migrations directory not found: {self.migrations_dir}")
            return
        
        migration_files = sorted([
            f for f in os.listdir(self.migrations_dir) 
            if f.endswith('.sql') and f.startswith('001_')
        ])
        
        if not migration_files:
            logger.info("No migration files found")
            return
        
        logger.info(f"Found {len(migration_files)} migration files")
        
        for migration_file in migration_files:
            try:
                self.run_migration(migration_file)
            except Exception as e:
                logger.error(f"Failed to run migration {migration_file}: {e}")
                raise
        
        logger.info("All migrations completed successfully")


def main():
    """Main entry point for running migrations."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        migration_manager = MigrationManager()
        migration_manager.run_all_migrations()
        print("✅ Database migrations completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        print(f"❌ Migration failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
