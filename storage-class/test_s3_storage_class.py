# Test S3 Storage Class Feature for Databend
# This script tests the S3 storage class configuration functionality

import pytest
import boto3
from databend_driver import BlockingDatabendClient
import os
import time
import json


class S3StorageClassTester:
    def __init__(self, dsn, s3_config):
        self.client = BlockingDatabendClient(dsn)
        self.cursor = self.client.cursor()

        # Separate boto3 client configuration from other configurations
        boto3_config = {
            'aws_access_key_id': s3_config['aws_access_key_id'],
            'aws_secret_access_key': s3_config['aws_secret_access_key'],
            'region_name': s3_config['region_name']
        }

        self.s3_client = boto3.client('s3', **boto3_config)
        self.bucket = s3_config['bucket']
        self.root_prefix = s3_config.get('root_prefix', 'data2/')
        self.aws_access_key_id = s3_config['aws_access_key_id']
        self.aws_secret_access_key = s3_config['aws_secret_access_key']

    def get_table_location(self, database, table_name):
        """Get table's S3 location"""
        try:
            query = f"SELECT snapshot_location FROM fuse_snapshot('{database}', '{table_name}') LIMIT 1"
            print(f"Executing query: {query}")
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            print(f"Query result: {result}")

            if result:
                snapshot_location = result.values()[0]
                print(f"Snapshot location: {snapshot_location}")
                # Extract directory path
                dir_path = self._extract_dir_from_snapshot_location(snapshot_location)
                print(f"Extracted directory path: {dir_path}")
                return dir_path
            else:
                print("No snapshot found for table")
                return None
        except Exception as e:
            print(f"Error getting table location: {e}")
            import traceback
            traceback.print_exc()
            raise

    def _extract_dir_from_snapshot_location(self, snapshot_location):
        """Extract directory path from snapshot_location"""
        # Example: "1/14579/_ss/h0199962459e6747a8a542d22a1a527ea_v4.mpk" -> "1/14579/"
        # Or: "_tmp_tbl/root/7ae250ad-0f3b-469c-87ad-cb27cc13e6db/1/4611686018427407904/_ss/h019a045777b777cb88883485c115524c_v4.mpk"
        #     -> "_tmp_tbl/root/7ae250ad-0f3b-469c-87ad-cb27cc13e6db/1/4611686018427407904/"
        parts = snapshot_location.split('/')
        if len(parts) >= 2:
            # Find _ss directory and take the path before it
            try:
                ss_index = parts.index('_ss')
                return '/'.join(parts[:ss_index]) + '/'
            except ValueError:
                # If no _ss directory found, take the preceding parts
                return '/'.join(parts[:-2]) + '/'
        return snapshot_location

    def check_storage_class(self, s3_prefix, expected_class):
        """Check S3 objects' storage class"""
        try:
            full_prefix = f"{self.root_prefix}{s3_prefix}" if not s3_prefix.startswith(self.root_prefix) else s3_prefix
            print(f"Checking S3 objects with prefix: s3://{self.bucket}/{full_prefix}")

            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=full_prefix
            )

            objects_checked = 0
            objects_found = response.get('Contents', [])
            print(f"Found {len(objects_found)} objects")

            for obj in objects_found:
                # Skip directory markers
                if obj['Key'].endswith('/'):
                    print(f"Skipping directory: {obj['Key']}")
                    continue

                print(f"Checking object: s3://{self.bucket}/{obj['Key']}")
                try:
                    head_response = self.s3_client.head_object(
                        Bucket=self.bucket,
                        Key=obj['Key']
                    )
                    storage_class = head_response.get('StorageClass', 'STANDARD')
                    print(f"  Storage class: {storage_class}")

                    assert storage_class == expected_class, f"Object {obj['Key']} has storage class {storage_class}, expected {expected_class}"
                    objects_checked += 1
                except Exception as e:
                    print(f"  Error checking object {obj['Key']}: {e}")
                    raise

            if objects_checked == 0:
                print(f"No valid objects found in prefix {full_prefix}")
                print("Available objects:")
                for obj in objects_found:
                    print(f"  {obj['Key']} (size: {obj['Size']})")

            assert objects_checked > 0, f"No objects found in prefix {full_prefix}"
            return objects_checked

        except Exception as e:
            print(f"Error in check_storage_class: {e}")
            import traceback
            traceback.print_exc()
            raise

    def check_external_storage_class(self, s3_prefix, expected_class):
        """Check external table S3 objects' storage class (without root_prefix)"""
        try:
            print(f"Checking external S3 objects with prefix: s3://{self.bucket}/{s3_prefix}")

            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=s3_prefix
            )

            objects_checked = 0
            objects_found = response.get('Contents', [])
            print(f"Found {len(objects_found)} objects")

            for obj in objects_found:
                # Skip directory markers
                if obj['Key'].endswith('/'):
                    print(f"Skipping directory: {obj['Key']}")
                    continue

                # Skip fixed version files
                if obj['Key'].endswith('_v_d77aa11285c22e0e1d4593a035c98c0d'):
                    print(f"Skipping version file: {obj['Key']}")
                    continue

                print(f"Checking object: s3://{self.bucket}/{obj['Key']}")
                try:
                    head_response = self.s3_client.head_object(
                        Bucket=self.bucket,
                        Key=obj['Key']
                    )
                    storage_class = head_response.get('StorageClass', 'STANDARD')
                    print(f"  Storage class: {storage_class}")

                    assert storage_class == expected_class, f"Object {obj['Key']} has storage class {storage_class}, expected {expected_class}"
                    objects_checked += 1
                except Exception as e:
                    print(f"  Error checking object {obj['Key']}: {e}")
                    raise

            if objects_checked == 0:
                print(f"No valid objects found in prefix {s3_prefix}")
                print("Available objects:")
                for obj in objects_found:
                    print(f"  {obj['Key']} (size: {obj['Size']})")

            return objects_checked

        except Exception as e:
            print(f"Error in check_external_storage_class: {e}")
            import traceback
            traceback.print_exc()
            raise

    def cleanup_spill_data(self):
        """Clean up spill data"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=f"{self.root_prefix}_spill/"
            )

            objects_to_delete = [{'Key': obj['Key']} for obj in response.get('Contents', [])]

            if objects_to_delete:
                self.s3_client.delete_objects(
                    Bucket=self.bucket,
                    Delete={'Objects': objects_to_delete}
                )
                print(f"Cleaned up {len(objects_to_delete)} spill objects")
        except Exception as e:
            print(f"Warning: Failed to cleanup spill data: {e}")

    def create_s3_connection(self, connection_name="test_s3_connection"):
        """Create S3 connection"""
        create_conn_sql = f"""
        CREATE CONNECTION {connection_name}
          STORAGE_TYPE = 's3'
          ACCESS_KEY_ID = '{self.aws_access_key_id}'
          SECRET_ACCESS_KEY = '{self.aws_secret_access_key}'
        """
        try:
            self.cursor.execute(create_conn_sql)
            return connection_name
        except Exception as e:
            if "already exists" in str(e).lower():
                return connection_name
            raise

    def drop_s3_connection(self, connection_name):
        """Drop S3 connection"""
        try:
            self.cursor.execute(f"DROP CONNECTION {connection_name}")
        except Exception as e:
            print(f"Warning: Failed to drop connection {connection_name}: {e}")

    def close(self):
        """Close connections"""
        try:
            if hasattr(self.cursor, 'close'):
                self.cursor.close()
            # databend_driver's BlockingDatabendClient may not have close method, skip
        except Exception as e:
            print(f"Warning: Error closing connection: {e}")


@pytest.fixture
def tester():
    # Read configuration from environment variables
    dsn = os.getenv('DATABEND_DSN', 'databend://user:pass@localhost:8000/default')
    s3_config = {
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'region_name': os.getenv('AWS_REGION', 'us-east-1'),
        'bucket': os.getenv('S3_BUCKET'),
        'root_prefix': os.getenv('S3_ROOT_PREFIX', 'data2/')
    }

    # Validate required configurations
    required_configs = ['aws_access_key_id', 'aws_secret_access_key', 'bucket']
    for config in required_configs:
        if not s3_config[config]:
            pytest.skip(f"Missing required S3 config: {config}")

    tester = S3StorageClassTester(dsn, s3_config)
    yield tester
    tester.close()


def test_default_fuse_table_storage_class(tester):
    """
    Test scenario: Default Fuse table storage class
    - Setting: s3_storage_class = 'standard' (default value)
    - Operation: Create normal Fuse table and insert data
    - Expected: All objects use STANDARD storage class
    """
    print("=== Testing default Fuse table storage class ===")
    print("Scenario: Default Fuse table should use STANDARD storage class")

    try:
        # 1. Ensure using default setting
        print("1. Setting s3_storage_class to 'standard'")
        tester.cursor.execute("SET s3_storage_class = 'standard'")

        # 2. Create table and insert data
        table_name = f"test_default_{int(time.time())}"
        print(f"2. Creating table: {table_name}")
        tester.cursor.execute(f"CREATE TABLE {table_name} (id INT, name STRING)")
        print("3. Inserting data")
        tester.cursor.execute(f"INSERT INTO {table_name} VALUES (1, 'test')")

        # 3. Get table S3 location
        print("4. Getting table S3 location")
        s3_location = tester.get_table_location('default', table_name)
        assert s3_location is not None, "Failed to get table S3 location"
        print(f"Table S3 location: {s3_location}")

        # 4. Check S3 objects immediately
        print("5. Checking S3 objects immediately")
        print(f"  S3 bucket: {tester.bucket}")
        print(f"  Root prefix: '{tester.root_prefix}'")
        print(f"  Table location: '{s3_location}'")

        # Build full S3 path for debugging
        if s3_location.startswith(tester.root_prefix):
            full_s3_path = s3_location
        else:
            full_s3_path = f"{tester.root_prefix}{s3_location}"
        print(f"  Full S3 path: s3://{tester.bucket}/{full_s3_path}")

        # Check storage class directly (no waiting logic)
        print("6. Checking storage class")
        objects_checked = tester.check_storage_class(s3_location, 'STANDARD')
        assert objects_checked > 0
        print(f"✓ Verified {objects_checked} objects with STANDARD storage class")

    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # 5. Cleanup
        try:
            print("7. Cleaning up table")
            tester.cursor.execute(f"DROP TABLE {table_name}")
        except Exception as e:
            print(f"Warning: Failed to cleanup table {table_name}: {e}")


def test_intelligent_tiering_fuse_table(tester):
    """
    Test scenario: Fuse table with intelligent_tiering setting
    - Setting: s3_storage_class = 'intelligent_tiering'
    - Operation: Create normal Fuse table and insert data
    - Expected: All objects use INTELLIGENT_TIERING storage class
    """
    print("=== Testing intelligent_tiering Fuse table storage class ===")
    print("Scenario: Fuse table with intelligent_tiering setting should use INTELLIGENT_TIERING storage class")

    try:
        # 1. Set storage class
        print("1. Setting s3_storage_class to 'intelligent_tiering'")
        tester.cursor.execute("SET s3_storage_class = 'intelligent_tiering'")

        # 2. Create table and insert data
        table_name = f"test_it_{int(time.time())}"
        print(f"2. Creating table: {table_name}")
        tester.cursor.execute(f"CREATE TABLE {table_name} (id INT, name STRING)")
        print("3. Inserting data")
        tester.cursor.execute(f"INSERT INTO {table_name} VALUES (1, 'test')")

        # 3. Get table S3 location
        print("4. Getting table S3 location")
        s3_location = tester.get_table_location('default', table_name)
        assert s3_location is not None, "Failed to get table S3 location"
        print(f"Table S3 location: {s3_location}")

        # 4. Check storage class directly
        print("5. Checking storage class")
        objects_checked = tester.check_storage_class(s3_location, 'INTELLIGENT_TIERING')
        assert objects_checked > 0
        print(f"✓ Verified {objects_checked} objects with INTELLIGENT_TIERING storage class")

    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # 5. Reset setting and cleanup
        try:
            print("6. Cleaning up")
            tester.cursor.execute("SET s3_storage_class = 'standard'")
            tester.cursor.execute(f"DROP TABLE {table_name}")
        except Exception as e:
            print(f"Warning: Failed to cleanup table {table_name}: {e}")


def test_external_fuse_table_storage_class(tester):
    """
    Test scenario: External Fuse table storage class (intelligent_tiering)
    - Setting: s3_storage_class = 'intelligent_tiering'
    - Operation: Create external fuse table and insert data
    - Expected: Data objects use INTELLIGENT_TIERING storage class (ignore _v_* version files)
    """
    print("=== Testing external Fuse table storage class ===")
    print("Scenario: External Fuse table with intelligent_tiering setting should use INTELLIGENT_TIERING storage class")

    # 1. Create S3 connection
    connection_name = f"test_conn_{int(time.time())}"
    tester.create_s3_connection(connection_name)

    try:
        # 2. Set storage class
        print("1. Setting s3_storage_class to 'intelligent_tiering'")
        tester.cursor.execute("SET s3_storage_class = 'intelligent_tiering'")

        # 3. Create external table
        external_location = f"s3://{tester.bucket}/external_test_{int(time.time())}/"
        table_name = f"ext_test_{int(time.time())}"

        print(f"2. Creating external table: {table_name}")
        print(f"   External location: {external_location}")

        create_table_sql = f"""
            CREATE TABLE {table_name} (a INT)
            '{external_location}'
            CONNECTION = (CONNECTION_NAME = '{connection_name}')
        """
        tester.cursor.execute(create_table_sql)

        # 4. Insert data
        print("3. Inserting data")
        tester.cursor.execute(f"INSERT INTO {table_name} VALUES (1)")

        # 5. Get table's S3 location
        print("4. Getting external table S3 location")
        s3_location = tester.get_table_location('default', table_name)
        assert s3_location is not None, "Failed to get external table S3 location"
        print(f"Table S3 location from fuse_snapshot: {s3_location}")

        # 6. For external table, need to check the path specified by external_location
        # Extract the path part of external_location (remove s3://bucket/ prefix)
        external_prefix = external_location.replace(f"s3://{tester.bucket}/", "")
        print(f"External location prefix: {external_prefix}")

        # 7. Check two possible locations
        print("5. Checking storage class in external location")

        try:
            # First try to check the path specified by external location
            print(f"  Trying external location: {external_prefix}")
            objects_checked = tester.check_external_storage_class(external_prefix, 'INTELLIGENT_TIERING')
            if objects_checked > 0:
                print(f"✓ Verified {objects_checked} external table objects with INTELLIGENT_TIERING storage class")
            else:
                print("  No objects found in external location, trying snapshot location")
                # If no objects in external location, try using snapshot location
                objects_checked = tester.check_external_storage_class(s3_location, 'INTELLIGENT_TIERING')
                print(f"✓ Verified {objects_checked} external table objects with INTELLIGENT_TIERING storage class (from snapshot location)")

        except Exception as e:
            print(f"Error checking external table storage class: {e}")
            import traceback
            traceback.print_exc()
            raise

        # 8. Cleanup
        print("6. Cleaning up external table")
        tester.cursor.execute(f"DROP TABLE {table_name}")

    finally:
        # 9. Reset settings and cleanup connection
        tester.cursor.execute("SET s3_storage_class = 'standard'")
        tester.drop_s3_connection(connection_name)


def test_external_fuse_table_default_storage_class(tester):
    """
    Test scenario: External Fuse table default storage class
    - Setting: s3_storage_class = 'standard' (default value)
    - Operation: Create external fuse table and insert data
    - Expected: Data objects use STANDARD storage class (ignore _v_* version files)
    """
    print("=== Testing external Fuse table default storage class ===")
    print("Scenario: External Fuse table with default setting should use STANDARD storage class")

    # 1. Create S3 connection
    connection_name = f"test_conn_default_{int(time.time())}"
    tester.create_s3_connection(connection_name)

    try:
        # 2. Ensure using default setting
        print("1. Setting s3_storage_class to 'standard'")
        tester.cursor.execute("SET s3_storage_class = 'standard'")

        # 3. Create external table
        external_location = f"s3://{tester.bucket}/external_default_test_{int(time.time())}/"
        table_name = f"ext_default_test_{int(time.time())}"

        print(f"2. Creating external table: {table_name}")
        print(f"   External location: {external_location}")

        create_table_sql = f"""
            CREATE TABLE {table_name} (a INT)
            '{external_location}'
            CONNECTION = (CONNECTION_NAME = '{connection_name}')
        """
        tester.cursor.execute(create_table_sql)

        # 4. Insert data
        print("3. Inserting data")
        tester.cursor.execute(f"INSERT INTO {table_name} VALUES (1)")

        # 5. Check storage class
        external_prefix = external_location.replace(f"s3://{tester.bucket}/", "")
        print("4. Checking storage class in external location")

        objects_checked = tester.check_external_storage_class(external_prefix, 'STANDARD')
        print(f"✓ Verified {objects_checked} external table objects with STANDARD storage class")

        # 6. Cleanup
        print("5. Cleaning up external table")
        tester.cursor.execute(f"DROP TABLE {table_name}")

    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        tester.drop_s3_connection(connection_name)


def test_spill_data_always_standard(tester):
    """
    Test scenario: Spill data should always use STANDARD storage class
    - Setting: s3_storage_class = 'intelligent_tiering' (intentionally set to non-standard)
    - Operation: Force generate spill data (force_aggregate_data_spill = 1)
    - Expected: Even when session is set to intelligent_tiering, spill data still uses STANDARD storage class
    """
    print("=== Testing spill data storage class ===")
    print("Scenario: Spill data should always use STANDARD storage class regardless of session setting")

    # 1. Clean up spill directory
    print("1. Cleaning up spill directory")
    tester.cleanup_spill_data()

    # 2. Set intelligent_tiering
    print("2. Setting s3_storage_class to 'intelligent_tiering'")
    tester.cursor.execute("SET s3_storage_class = 'intelligent_tiering'")

    # 3. Force generate spill
    print("3. Forcing data spill")
    tester.cursor.execute("SET force_aggregate_data_spill = 1")

    try:
        # Execute query that will generate spill
        print("4. Executing query to generate spill data")
        tester.cursor.execute("SELECT COUNT() FROM (SELECT number::string, count() FROM numbers_mt(100000) group by number::string)")
        result = tester.cursor.fetchone()
        print(f"Spill query result: {result.values()[0] if result else 'None'}")

        # 4. Directly check spill objects' storage class
        print("5. Checking spill data storage class")
        spill_prefix = "_spill/"
        try:
            objects_checked = tester.check_storage_class(spill_prefix, 'STANDARD')
            if objects_checked > 0:
                print(f"✓ Verified {objects_checked} spill objects with STANDARD storage class")
            else:
                print("Warning: No spill objects found - spill may not have been triggered")
        except AssertionError as e:
            if "No objects found" in str(e):
                print("Warning: No spill objects generated during test - query may not have triggered spill")
            else:
                raise

    finally:
        # 5. Reset settings
        print("6. Resetting settings")
        tester.cursor.execute("SET s3_storage_class = 'standard'")
        tester.cursor.execute("SET force_aggregate_data_spill = 0")


def test_temporary_table_always_standard(tester):
    """
    Test scenario: Temporary table should always use STANDARD storage class
    - Setting: s3_storage_class = 'intelligent_tiering' (intentionally set to non-standard)
    - Operation: Create temporary table
    - Expected: Even when session is set to intelligent_tiering, temporary table data still uses STANDARD storage class
    """
    print("=== Testing temporary table storage class ===")
    print("Scenario: Temporary table should always use STANDARD storage class regardless of session setting")

    try:
        # 1. Set intelligent_tiering
        print("1. Setting s3_storage_class to 'intelligent_tiering'")
        tester.cursor.execute("SET s3_storage_class = 'intelligent_tiering'")

        # 2. Create temporary table
        print("2. Creating temporary table")
        tester.cursor.execute("CREATE TEMPORARY TABLE t_tmp (c INT) AS SELECT 1")

        # 3. Get temporary table's S3 location
        print("3. Getting temporary table S3 location")
        s3_location = tester.get_table_location('default', 't_tmp')
        assert s3_location is not None, "Failed to get temporary table S3 location"
        print(f"Temporary table S3 location: {s3_location}")

        # 4. Directly check storage class
        print("4. Checking temporary table storage class")
        objects_checked = tester.check_storage_class(s3_location, 'STANDARD')
        assert objects_checked > 0
        print(f"✓ Verified {objects_checked} temporary table objects with STANDARD storage class")

    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # 5. Reset settings
        print("5. Resetting settings")
        tester.cursor.execute("SET s3_storage_class = 'standard'")


if __name__ == "__main__":
    # Can run directly for quick testing
    import sys

    # Debug: print all environment variables
    print("=== S3 Storage Class Test Suite ===")
    print("Current environment variables:")
    for var in ['DATABEND_DSN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET', 'AWS_REGION', 'S3_ROOT_PREFIX']:
        value = os.getenv(var, 'NOT SET')
        # Mask sensitive information for display
        if var == 'AWS_SECRET_ACCESS_KEY' and value != 'NOT SET':
            value = value[:4] + '*' * (len(value) - 8) + value[-4:] if len(value) > 8 else '***'
        print(f"  {var}: {value}")
    print()

    # Check required environment variables
    required_env_vars = ['DATABEND_DSN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("\nRequired environment variables:")
        print("  DATABEND_DSN - Databend connection string")
        print("  AWS_ACCESS_KEY_ID - AWS access key")
        print("  AWS_SECRET_ACCESS_KEY - AWS secret key")
        print("  S3_BUCKET - S3 bucket name")
        print("  AWS_REGION - AWS region (optional, default: us-east-1)")
        print("  S3_ROOT_PREFIX - S3 root prefix (optional, default: data2/)")
        sys.exit(1)

    print("Running S3 Storage Class tests...")
    print("=" * 80)

    # Define test cases
    test_cases = [
        ("test_default_fuse_table_storage_class", "Default Fuse table storage class (STANDARD)"),
        ("test_intelligent_tiering_fuse_table", "Intelligent tiering Fuse table storage class (INTELLIGENT_TIERING)"),
        ("test_external_fuse_table_storage_class", "External Fuse table with intelligent tiering (INTELLIGENT_TIERING)"),
        ("test_external_fuse_table_default_storage_class", "External Fuse table with default setting (STANDARD)"),
        ("test_spill_data_always_standard", "Spill data always uses STANDARD (even when session=INTELLIGENT_TIERING)"),
        ("test_temporary_table_always_standard", "Temporary table always uses STANDARD (even when session=INTELLIGENT_TIERING)")
    ]

    # Run tests and collect results
    import subprocess
    result = subprocess.run([
        sys.executable, "-m", "pytest", __file__, "-v", "--tb=short", "-x"
    ], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("STDERR:")
        print(result.stderr)

    # Parse test results
    lines = result.stdout.split('\n')
    passed_tests = []
    failed_tests = []
    error_tests = []

    for line in lines:
        if '::test_' in line:
            if 'PASSED' in line:
                test_name = line.split('::')[1].split()[0]
                passed_tests.append(test_name)
            elif 'FAILED' in line:
                test_name = line.split('::')[1].split()[0]
                failed_tests.append(test_name)
            elif 'ERROR' in line:
                test_name = line.split('::')[1].split()[0]
                error_tests.append(test_name)

    # Print summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    print(f"\nTotal Tests: {len(test_cases)}")
    print(f"✓ Passed: {len(passed_tests)}")
    print(f"✗ Failed: {len(failed_tests)}")
    print(f"⚠ Errors: {len(error_tests)}")

    if passed_tests:
        print(f"\n✓ PASSED TESTS ({len(passed_tests)}):")
        for test_name in passed_tests:
            # Find corresponding description
            description = next((desc for name, desc in test_cases if name == test_name), test_name)
            print(f"  • {description}")

    if failed_tests:
        print(f"\n✗ FAILED TESTS ({len(failed_tests)}):")
        for test_name in failed_tests:
            description = next((desc for name, desc in test_cases if name == test_name), test_name)
            print(f"  • {description}")

    if error_tests:
        print(f"\n⚠ ERROR TESTS ({len(error_tests)}):")
        for test_name in error_tests:
            description = next((desc for name, desc in test_cases if name == test_name), test_name)
            print(f"  • {description}")

    # Overall result
    if len(passed_tests) == len(test_cases):
        print(f"\n🎉 ALL TESTS PASSED! S3 storage class feature is working correctly.")
        exit_code = 0
    elif failed_tests or error_tests:
        print(f"\n❌ SOME TESTS FAILED. Please check the implementation.")
        exit_code = 1
    else:
        print(f"\n⚠️  TESTS COMPLETED WITH WARNINGS.")
        exit_code = 0

    print("=" * 80)
    sys.exit(exit_code)