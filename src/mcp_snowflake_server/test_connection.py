import asyncio
import os
import dotenv
from db_client import SnowflakeDB

async def main():
    # Load environment variables
    dotenv.load_dotenv()
    
    # Get connection parameters from environment variables
    connection_config = {
        k.lower(): os.getenv(f"SNOWFLAKE_{k.upper()}")
        for k in ["account", "user", "database", "schema", "warehouse", "role"]
        if os.getenv(f"SNOWFLAKE_{k.upper()}")
    }
    
    # Add private key path and passphrase if they exist
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
        connection_config["private_key_path"] = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        connection_config["private_key_passphrase"] = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    
    # Create database client
    db = SnowflakeDB(connection_config)
    
    # Test connection
    success, message = await db.test_connection()
    print(message)

if __name__ == "__main__":
    asyncio.run(main()) 