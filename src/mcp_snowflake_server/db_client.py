import asyncio
import logging
import time
import uuid
from typing import Any
import snowflake.connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("mcp_snowflake_server")


class SnowflakeDB:
    AUTH_EXPIRATION_TIME = 1800

    def __init__(self, connection_config: dict):
        self.connection_config = connection_config
        self.connection = None
        self.cursor = None
        self.insights: list[str] = []
        self.auth_time = 0
        self.init_task = None  # To store the task reference

    async def _init_database(self):
        """Initialize connection to the Snowflake database"""
        try:
            # Configure connection parameters
            conn_params = {
                'account': self.connection_config.get('account'),
                'user': self.connection_config.get('user'),
                'authenticator': 'SNOWFLAKE_JWT',
                'private_key_file': self.connection_config.get('private_key_path'),
                'private_key_file_pwd': self.connection_config.get('private_key_passphrase'),
                'warehouse': self.connection_config.get('warehouse'),
                'database': self.connection_config.get('database'),
                'schema': self.connection_config.get('schema'),
                'role': self.connection_config.get('role')
            }

            # Create connection
            self.connection = snowflake.connector.connect(**conn_params)
            self.cursor = self.connection.cursor()
            self.auth_time = time.time()

        except Exception as e:
            raise ValueError(f"Failed to connect to Snowflake database: {e}")

    async def test_connection(self) -> tuple[bool, str]:
        """Test the connection to Snowflake and list available tables"""
        try:
            # If init_task exists and isn't done, wait for it to complete
            if self.init_task and not self.init_task.done():
                await self.init_task
            # If connection doesn't exist or has expired, initialize it and wait
            elif not self.connection or time.time() - self.auth_time > self.AUTH_EXPIRATION_TIME:
                await self._init_database()

            # Test connection by getting current role and database
            self.cursor.execute("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            role, database, schema = self.cursor.fetchone()

            # Get list of tables
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()
            table_list = [table[1] for table in tables]  # Table name is in the second column

            return True, f"Connection successful!\nRole: {role}\nDatabase: {database}\nSchema: {schema}\n\nAvailable tables:\n" + "\n".join(f"- {table}" for table in table_list)

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False, f"Connection test failed: {e}"

    def start_init_connection(self):
        """Start database initialization in the background"""
        # Create a task that runs in the background
        loop = asyncio.get_event_loop()
        self.init_task = loop.create_task(self._init_database())
        return self.init_task

    async def execute_query(self, query: str) -> tuple[list[dict[str, Any]], str]:
        """Execute a SQL query and return results as a list of dictionaries"""
        # If init_task exists and isn't done, wait for it to complete
        if self.init_task and not self.init_task.done():
            await self.init_task
        # If connection doesn't exist or has expired, initialize it and wait
        elif not self.connection or time.time() - self.auth_time > self.AUTH_EXPIRATION_TIME:
            await self._init_database()

        logger.debug(f"Executing query: {query}")
        try:
            self.cursor.execute(query)
            columns = [desc[0] for desc in self.cursor.description]
            result_rows = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            data_id = str(uuid.uuid4())

            return result_rows, data_id

        except Exception as e:
            logger.error(f'Database error executing "{query}": {e}')
            raise

    def add_insight(self, insight: str) -> None:
        """Add a new insight to the collection"""
        self.insights.append(insight)

    def get_memo(self) -> str:
        """Generate a formatted memo from collected insights"""
        if not self.insights:
            return "No data insights have been discovered yet."

        memo = "📊 Data Intelligence Memo 📊\n\n"
        memo += "Key Insights Discovered:\n\n"
        memo += "\n".join(f"- {insight}" for insight in self.insights)

        if len(self.insights) > 1:
            memo += f"\n\nSummary:\nAnalysis has revealed {len(self.insights)} key data insights that suggest opportunities for strategic optimization and growth."

        return memo
