"""Create database indexes for better query performance."""
import logging
from sqlalchemy import text
from src.common.database import get_db_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_indexes():
    """Create indexes on frequently queried columns."""
    indexes = [
        # Stock prices indexes
        ("idx_prices_stock_date", "stock_prices", "(stock_id, trade_date)"),
        ("idx_prices_date", "stock_prices", "(trade_date)"),
        ("idx_prices_stock_date_desc", "stock_prices", "(stock_id, trade_date DESC)"),

        # Institutional flows indexes
        ("idx_flows_stock_date", "institutional_flows", "(stock_id, trade_date)"),
        ("idx_flows_date", "institutional_flows", "(trade_date)"),

        # Foreign holdings indexes
        ("idx_holdings_stock_date", "foreign_holdings", "(stock_id, trade_date)"),

        # Institutional ratios indexes
        ("idx_ratios_stock_date", "institutional_ratios", "(stock_id, trade_date)"),

        # Strategy rankings indexes
        ("idx_rankings_metric", "strategy_rankings", "(metric_type)"),
        ("idx_rankings_tier_metric", "strategy_rankings", "(price_tier, metric_type)"),

        # Margin trading indexes
        ("idx_margin_stock_date", "margin_trading", "(stock_id, trade_date)"),
        ("idx_margin_date", "margin_trading", "(trade_date)"),
        ("idx_margin_ratio", "margin_trading", "(short_margin_ratio DESC)"),

        # Monthly revenue indexes
        ("idx_revenue_stock_year_month", "monthly_revenue", "(stock_id, year, month)"),
        ("idx_revenue_year_month", "monthly_revenue", "(year, month)"),
    ]

    with get_db_session() as session:
        for idx_name, table, columns in indexes:
            try:
                # Check if index exists
                check_sql = text(f"""
                    SELECT 1 FROM pg_indexes
                    WHERE indexname = :idx_name
                """)
                result = session.execute(check_sql, {"idx_name": idx_name})

                if result.fetchone():
                    logger.info(f"  Index {idx_name} already exists, skipping")
                    continue

                # Create index
                create_sql = text(f"CREATE INDEX {idx_name} ON {table} {columns}")
                session.execute(create_sql)
                session.commit()
                logger.info(f"  Created index {idx_name} on {table}")

            except Exception as e:
                logger.error(f"  Failed to create index {idx_name}: {e}")
                session.rollback()

    logger.info("Index creation completed")


def migrate_revenue_columns():
    """Migrate revenue columns to allow larger values."""
    migrations = [
        ("mom_change", "NUMERIC(15, 4)"),
        ("yoy_change", "NUMERIC(15, 4)"),
        ("cumulative_yoy_change", "NUMERIC(15, 4)"),
    ]

    with get_db_session() as session:
        for column, new_type in migrations:
            try:
                # Check current column type
                check_sql = text("""
                    SELECT data_type, numeric_precision
                    FROM information_schema.columns
                    WHERE table_name = 'monthly_revenue' AND column_name = :col
                """)
                result = session.execute(check_sql, {"col": column}).fetchone()

                if result and result[1] and result[1] >= 15:
                    logger.info(f"  Column {column} already has precision >= 15, skipping")
                    continue

                # Alter column type
                alter_sql = text(f"ALTER TABLE monthly_revenue ALTER COLUMN {column} TYPE {new_type}")
                session.execute(alter_sql)
                session.commit()
                logger.info(f"  Migrated column {column} to {new_type}")

            except Exception as e:
                logger.error(f"  Failed to migrate column {column}: {e}")
                session.rollback()

    logger.info("Revenue column migration completed")


if __name__ == "__main__":
    logger.info("Creating database indexes...")
    create_indexes()
    logger.info("Migrating revenue columns...")
    migrate_revenue_columns()
