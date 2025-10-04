import teradatasql
import os
from pathlib import Path


def install_CompleteChat(install_database: str = "openai_client"):
    """
    Install CompleteChat function in Teradata using dbc user.

    Args:
        install_database: Target database name (default: "openai_client")
    """
    # Get connection parameters from environment
    host = os.getenv("TD_HOST")
    user = "dbc"
    password = os.getenv("TD_DBC_PASSWORD")

    if not all([host, password]):
        raise ValueError("TD_HOST and TD_DBC_PASSWORD must be set as environment variables")

    # Get path to JAR file
    jar_path = Path(__file__).parent / "openai.client-1.0.2.jar"
    if not jar_path.exists():
        raise FileNotFoundError(f"JAR file not found at {jar_path}")

    # Connect to Teradata
    with teradatasql.connect(
        host=host,
        user=user,
        password=password,
        encryptdata="true"
    ) as conn:
        with conn.cursor() as cur:
            # Get number of AMPs
            cur.execute("SELECT HASHAMP()+1 as num_amps")
            num_amps = cur.fetchone()[0]
            perm_size = 8000000 * num_amps

            print(f"Number of AMPs: {num_amps}")
            print(f"Creating database {install_database} with PERM = {perm_size}")

            # Create database
            cur.execute(f"""
                CREATE DATABASE {install_database} AS PERM = {perm_size}
            """)

            # Set current database
            cur.execute(f"DATABASE {install_database}")

            # Grant permissions
            cur.execute(f"GRANT CREATE EXTERNAL PROCEDURE ON {install_database} TO dbc")
            cur.execute(f"GRANT CREATE FUNCTION ON {install_database} TO dbc")

            print(f"Database {install_database} created and permissions granted")

            # Install JAR
            print("Installing JAR file...")
            cur.execute(f"""
                CALL SQLJ.INSTALL_JAR(
                    'cj!{jar_path.absolute()}',
                    'OPENAI_CLIENT',
                    0
                )
            """)

            print("JAR installed successfully")

            # Create/replace function
            print("Creating CompleteChat function...")
            cur.execute(f"""
                REPLACE FUNCTION {install_database}.CompleteChat()
                RETURNS TABLE VARYING USING FUNCTION OpenAIClientTO_contract
                LANGUAGE JAVA
                NO SQL
                PARAMETER STYLE SQLTable
                EXTERNAL NAME 'OPENAI_CLIENT:com.teradata.openai.client.OpenAIClientTO.execute()'
            """)

            print(f"CompleteChat function created successfully in {install_database}")
            print("\nInstallation complete!")

def grant_execution_rights(install_database: str = "openai_client", target_database: str = "demo_user"):
    """
    Grant execution rights for CompleteChat function to a target database/user.

    Args:
        install_database: Database where CompleteChat is installed (default: "openai_client")
        target_database: Database/user to grant execution rights to (default: "demo_user")
    """
    # Get connection parameters from environment
    host = os.getenv("TD_HOST")
    user = "dbc"
    password = os.getenv("TD_DBC_PASSWORD")

    if not all([host, password]):
        raise ValueError("TD_HOST and TD_DBC_PASSWORD must be set as environment variables")

    # Connect to Teradata
    with teradatasql.connect(
        host=host,
        user=user,
        password=password,
        encryptdata="true"
    ) as conn:
        with conn.cursor() as cur:
            # Grant execution rights
            print(f"Granting execution rights on {install_database}.CompleteChat to {target_database}...")
            cur.execute(f"GRANT EXECUTE FUNCTION ON {install_database}.CompleteChat TO {target_database}")
            print(f"Execution rights granted successfully to {target_database}")

def uninstall_CompleteChat(install_database: str = "openai_client"):
    """
    Uninstall CompleteChat function and remove the database from Teradata.

    Args:
        install_database: Database where CompleteChat is installed (default: "openai_client")
    """
    # Get connection parameters from environment
    host = os.getenv("TD_HOST")
    user = "dbc"
    password = os.getenv("TD_DBC_PASSWORD")

    if not all([host, password]):
        raise ValueError("TD_HOST and TD_DBC_PASSWORD must be set as environment variables")

    # Connect to Teradata
    with teradatasql.connect(
        host=host,
        user=user,
        password=password,
        encryptdata="true"
    ) as conn:
        with conn.cursor() as cur:
            # Drop the function first
            print(f"Dropping CompleteChat function from {install_database}...")
            try:
                cur.execute(f"DROP FUNCTION {install_database}.CompleteChat")
                print("CompleteChat function dropped successfully")
            except Exception as e:
                print(f"Warning: Could not drop function - {e}")

            # Drop all objects in the database first
            print(f"Dropping all objects in {install_database}...")
            try:
                cur.execute(f"DELETE DATABASE {install_database}")
                print(f"  Deleted all objects in database {install_database}")
            except Exception as e:
                print(f"  Warning: Could not delete contents of database  {install_database} - {e}")

            # Drop the database
            print(f"Dropping database {install_database}...")
            try:
                cur.execute(f"DROP DATABASE {install_database}")
                print(f"Database {install_database} dropped successfully")
            except Exception as e:
                print(f"Warning: Could not drop database - {e}")

            print("\nUninstallation complete!")

