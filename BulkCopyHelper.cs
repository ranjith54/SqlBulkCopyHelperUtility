using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace SqlBulkCopyHelperUtility
{
    /// <summary>
    /// Helper class for performing bulk insert operations into a SQL Server database using SqlBulkCopy.
    /// This utility converts multiple generic collections of data into DataTables and efficiently 
    /// inserts them into their respective SQL tables, supporting transactions.
    /// </summary>
    public class BulkCopyHelper
    {
        /// <summary>
        /// Inserts multiple collections of data into their respective SQL tables using bulk copy with an external transaction.
        /// The transaction is managed by the caller and should be committed or rolled back by the caller.
        /// </summary>
        /// <param name="dataDictionary">A dictionary where the key is the table name and the value is the collection of data to be inserted into that table.</param>
        /// <param name="connection">An open SqlConnection to the SQL Server database.</param>
        /// <param name="sqlTransaction">An existing SqlTransaction under which the bulk insert should be executed. The caller manages the transaction lifecycle.</param>
        public static void BulkInsertMultipleTables(Dictionary<string, IEnumerable<object>> dataDictionary, SqlConnection connection, SqlTransaction sqlTransaction = null)
        {
            foreach (var entry in dataDictionary)
            {
                string tableName = entry.Key;
                IEnumerable<object> data = entry.Value;
                ValidateData(data);
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, sqlTransaction))
                {
                    bulkCopy.BulkCopyTimeout = 60; // Set appropriate timeout
                    bulkCopy.BatchSize = 1000; // Set appropriate batch size
                    bulkCopy.DestinationTableName = tableName;

                    // Convert the data to a DataTable
                    DataTable table = ToDataTable(data);

                    // Get the existing column names from the database
                    var existingColumns = GetColumnNamesFromDatabase(tableName, connection, sqlTransaction);

                    // Map the columns from the DataTable to the destination SQL table
                    foreach (var column in table.Columns.Cast<DataColumn>())
                    {
                        if (existingColumns.Contains(column.ColumnName))
                        {
                            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                        }
                    }

                    bulkCopy.WriteToServer(table);

                }
            }
        }

        /// <summary>
        /// Converts a collection of data into a DataTable. 
        /// Each property of the generic type T becomes a column in the DataTable, 
        /// and each object in the collection becomes a row.
        /// </summary>
        /// <param name="data">The collection of data to be converted into a DataTable.</param>
        /// <returns>A DataTable representation of the collection where each property of T becomes a column, and each object is a row.</returns>
        private static DataTable ToDataTable(IEnumerable<object> data)
        {
            DataTable table = new DataTable();

            if (data == null || !data.Any())
            {
                throw new ArgumentException("Data cannot be null or empty.");
            }

            // Get the type of the first object in the collection
            Type type = data.First().GetType();

            // Get all public instance properties of the type
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            // Create columns in the DataTable for each property
            foreach (var prop in properties)
            {
                var columnType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                table.Columns.Add(GetColumnName(prop.Name, data), columnType);
            }

            // Populate the DataTable rows with data from the collection
            foreach (var item in data)
            {
                var values = new object[properties.Length];
                for (int i = 0; i < properties.Length; i++)
                {
                    var value = properties[i].GetValue(item, null);
                    // Default value handling
                    values[i] = value ?? (Nullable.GetUnderlyingType(properties[i].PropertyType) != null ? Activator.CreateInstance(properties[i].PropertyType) : value);
                }
                table.Rows.Add(values);
            }

            return table;
        }

        /// <summary>
        /// Validates the data to ensure no null values exist in non-nullable properties.
        /// Throws an exception if a non-nullable property has a null value.
        /// </summary>
        /// <param name="data">The collection of data to validate.</param>
        private static void ValidateData(IEnumerable<object> data)
        {
            foreach (var item in data)
            {
                var properties = item.GetType().GetProperties();
                foreach (var prop in properties)
                {
                    var value = prop.GetValue(item);
                    if (value == null && !IsNullableType(prop.PropertyType))
                    {
                        throw new InvalidOperationException($"Property '{prop.Name}' cannot be null.");
                    }
                }
            }
        }

        /// <summary>
        /// Retrieves the column names from the specified SQL table.
        /// </summary>
        /// <param name="tableName">The name of the SQL table.</param>
        /// <param name="connection">An open SqlConnection to the SQL Server database.</param>
        /// <param name="sqlTransaction">An existing SqlTransaction under which the query should be executed.</param>
        /// <returns>A list of column names in the specified SQL table.</returns>
        private static List<string> GetColumnNamesFromDatabase(string tableName, SqlConnection connection, SqlTransaction sqlTransaction = null)
        {
            var columnNames = new List<string>();
            string query = @"
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = @TableName";

            using (var command = new SqlCommand(query, connection, sqlTransaction))
            {
                command.Parameters.AddWithValue("@TableName", tableName);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columnNames.Add(reader.GetString(0)); // Add column name to the list
                    }
                }
            }

            return columnNames;
        }

        /// <summary>
        /// Retrieves the column name from the property attributes.
        /// If the property is decorated with a ColumnAttribute, the name from the attribute is returned.
        /// If not, the original property name is returned.
        /// </summary>
        /// <param name="propertyName">The name of the property in the class.</param>
        /// <param name="data">The collection of data to be inserted (used to get type information).</param>
        /// <returns>The column name in the SQL table.</returns>
        public static string GetColumnName(string propertyName, IEnumerable<object> data)
        {
            // Get the type of the class
            Type type = data.First().GetType();

            // Get the property info for the specified property name
            PropertyInfo propertyInfo = type.GetProperty(propertyName);

            if (propertyInfo != null)
            {
                // Get the ColumnAttribute applied to the property
                var columnAttribute = propertyInfo.GetCustomAttributes(typeof(ColumnAttribute), false)
                                                   .FirstOrDefault() as ColumnAttribute;

                if (columnAttribute != null)
                {
                    // Return the Name property of the ColumnAttribute
                    return columnAttribute.Name;
                }
            }

            return null; // Or throw an exception if you prefer
        }

        /// <summary>
        /// Determines if a given type is nullable. 
        /// For value types, it checks if the type is a nullable value type.
        /// Reference types are always nullable.
        /// </summary>
        /// <param name="type">The type to check for nullability.</param>
        /// <returns>True if the type is nullable, otherwise false.</returns>
        public static bool IsNullableType(Type type)
        {
            // Check if the type is a value type
            if (type.IsValueType)
            {
                // Check if it is a nullable value type
                return Nullable.GetUnderlyingType(type) != null;
            }
            // Reference types are always nullable
            return true;
        }
    }
}