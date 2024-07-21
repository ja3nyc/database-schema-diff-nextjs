import { Pool } from 'pg';

export interface ColumnInfo {
  type: string;
  maxLength: number | null;
  isNullable: boolean;
  defaultValue: string | null;
  isPrimaryKey: boolean;
  permissions: string[];
}

interface ConsolidatedPermissions {
  [role: string]: {
    [privilege: string]: string[];
  }
}

export interface ForeignKeyInfo {
  columnName: string;
  referenceTable: string;
  referenceColumn: string;
  updateRule: string;
  deleteRule: string;
}

export interface TableInfo {
  columns: { [columnName: string]: ColumnInfo };
  foreignKeys: ForeignKeyInfo[];
  rlsPolicies: string[];
}

export interface DatabaseSchema {
  [tableName: string]: TableInfo;
}

export interface ColumnInfo {
  type: string;
  maxLength: number | null;
  isNullable: boolean;
  defaultValue: string | null;
  isPrimaryKey: boolean;
}

export interface TableSchema {
  [columnName: string]: ColumnInfo;
}

export interface ColumnDiff {
  from: ColumnInfo;
  to: ColumnInfo;
}

export interface TableDiff {
  columnsAdded: string[];
  columnsRemoved: string[];
  columnsDiff: {
    [columnName: string]: ColumnDiff;
  };
}

export interface SchemaDiff {
  tablesAdded: string[];
  tablesRemoved: string[];
  tablesDiff: {
    [tableName: string]: TableDiff;
  };
}

export interface ColumnInfo {
  type: string;
  maxLength: number | null;
  isNullable: boolean;
  defaultValue: string | null;
  isPrimaryKey: boolean;
  permissions: string[];
}

export interface ForeignKeyInfo {
  columnName: string;
  referenceTable: string;
  referenceColumn: string;
  updateRule: string;
  deleteRule: string;
}

export interface TableInfo {
  columns: { [columnName: string]: ColumnInfo };
  foreignKeys: ForeignKeyInfo[];
  rlsPolicies: string[];
}


export interface ColumnDiff {
  from: ColumnInfo;
  to: ColumnInfo;
}

export interface TableDiff {
  columnsAdded: string[];
  columnsRemoved: string[];
  columnsDiff: { [columnName: string]: ColumnDiff };
  foreignKeys?: ForeignKeyInfo[];
  rlsPolicies?: string[];
}

export interface SchemaDiff {
  tablesAdded: string[];
  tablesRemoved: string[];
  tablesDiff: { [tableName: string]: TableDiff };
}

export async function compareSchemas(schema1: DatabaseSchema, schema2: DatabaseSchema): Promise<SchemaDiff> {
  const diff: SchemaDiff = {
    tablesAdded: Object.keys(schema2).filter(table => !schema1[table]),
    tablesRemoved: Object.keys(schema1).filter(table => !schema2[table]),
    tablesDiff: {},
  };

  for (const table in schema1) {
    if (schema2[table]) {
      const tableDiff: TableDiff = {
        columnsAdded: [],
        columnsRemoved: [],
        columnsDiff: {},
      };

      // Compare columns
      for (const col in schema2[table].columns) {
        if (!schema1[table].columns[col]) {
          tableDiff.columnsAdded.push(col);
        } else if (JSON.stringify(schema1[table].columns[col]) !== JSON.stringify(schema2[table].columns[col])) {
          tableDiff.columnsDiff[col] = {
            from: schema1[table].columns[col],
            to: schema2[table].columns[col],
          };
        }
      }

      for (const col in schema1[table].columns) {
        if (!schema2[table].columns[col]) {
          tableDiff.columnsRemoved.push(col);
        }
      }

      // Compare foreign keys
      if (JSON.stringify(schema1[table].foreignKeys) !== JSON.stringify(schema2[table].foreignKeys)) {
        tableDiff.foreignKeys = schema2[table].foreignKeys;
      }

      // Compare RLS policies
      if (JSON.stringify(schema1[table].rlsPolicies) !== JSON.stringify(schema2[table].rlsPolicies)) {
        tableDiff.rlsPolicies = schema2[table].rlsPolicies;
      }

      if (Object.keys(tableDiff.columnsDiff).length > 0 || tableDiff.columnsAdded.length > 0 ||
        tableDiff.columnsRemoved.length > 0 || tableDiff.foreignKeys || tableDiff.rlsPolicies) {
        diff.tablesDiff[table] = tableDiff;
      }
    }
  }

  return diff;
}

export async function getSchema(connectionString: string): Promise<DatabaseSchema> {
  const pool = new Pool({ connectionString });
  try {
    // Query for table and column information
    const columnsResult = await pool.query(`
      SELECT 
        c.table_name, 
        c.column_name, 
        c.data_type, 
        c.character_maximum_length,
        c.is_nullable,
        c.column_default,
        CASE WHEN pk.constraint_type = 'PRIMARY KEY' THEN TRUE ELSE FALSE END as is_primary_key
      FROM information_schema.columns c
      LEFT JOIN (
        SELECT ku.table_name, ku.column_name, tc.constraint_type
        FROM information_schema.key_column_usage ku
        JOIN information_schema.table_constraints tc 
          ON ku.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
      ) pk 
      ON c.table_name = pk.table_name AND c.column_name = pk.column_name
      WHERE c.table_schema = 'public'
      ORDER BY c.table_name, c.ordinal_position
    `);

    // Query for foreign key information
    const foreignKeysResult = await pool.query(`
      SELECT
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        rc.update_rule,
        rc.delete_rule
      FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints AS rc
          ON rc.constraint_name = tc.constraint_name
      WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
    `);

    // Query for RLS policies
    const rlsPoliciesResult = await pool.query(`
      SELECT tablename, policyname, cmd, qual, with_check
      FROM pg_policies
      WHERE schemaname = 'public'
    `);

    // Query for column-level permissions
    const columnPermissionsResult = await pool.query(`
      SELECT table_name, column_name, grantee, privilege_type
      FROM information_schema.column_privileges
      WHERE table_schema = 'public'
    `);

    const schema: DatabaseSchema = {};

    // Process column information
    for (const row of columnsResult.rows) {
      if (!schema[row.table_name]) {
        schema[row.table_name] = { columns: {}, foreignKeys: [], rlsPolicies: [] };
      }
      schema[row.table_name].columns[row.column_name] = {
        type: row.data_type,
        maxLength: row.character_maximum_length,
        isNullable: row.is_nullable === 'YES',
        defaultValue: row.column_default,
        isPrimaryKey: row.is_primary_key,
        permissions: [],
      };
    }

    // Process foreign key information
    for (const row of foreignKeysResult.rows) {
      if (schema[row.table_name]) {
        schema[row.table_name].foreignKeys.push({
          columnName: row.column_name,
          referenceTable: row.foreign_table_name,
          referenceColumn: row.foreign_column_name,
          updateRule: row.update_rule,
          deleteRule: row.delete_rule,
        });
      }
    }

    // Process RLS policies
    for (const row of rlsPoliciesResult.rows) {
      if (schema[row.tablename]) {
        schema[row.tablename].rlsPolicies.push(
          `CREATE POLICY ${row.policyname} ON ${row.tablename} FOR ${row.cmd} TO ${row.qual} WITH CHECK (${row.with_check})`
        );
      }
    }

    // Process column-level permissions
    for (const row of columnPermissionsResult.rows) {
      if (schema[row.table_name]?.columns[row.column_name]) {
        schema[row.table_name].columns[row.column_name].permissions.push(
          `GRANT ${row.privilege_type} ON ${row.table_name}(${row.column_name}) TO ${row.grantee}`
        );
      }
    }

    return schema;
  } finally {
    await pool.end();
  }
}

function getColumnDefinition(column: ColumnInfo): string {
  let def = column.type;
  if (column.maxLength) {
    def += `(${column.maxLength})`;
  }
  return def;
}

export function generatePsql(diff: SchemaDiff, fullSchema: DatabaseSchema): string {
  let psql = '';

  // Helper function to generate column definition
  const getColumnDefinitionSQL = (colName: string, colInfo: ColumnInfo): string => {
    let colDef = `${colName} ${getColumnDefinition(colInfo)}`;
    if (!colInfo.isNullable) colDef += ' NOT NULL';
    if (colInfo.defaultValue) colDef += ` DEFAULT ${colInfo.defaultValue}`;
    return colDef;
  };

  // Helper function to handle RLS policies
  const handleRLSPolicies = (table: string, policies: string[]): string => {
    let sql = `ALTER TABLE ${table} ENABLE ROW LEVEL SECURITY;\n`;
    policies.forEach(policy => {
      const [_, __, policyName, policyType, ___, policyAction, ...rest] = policy.split(' ');
      const usingIndex = rest.indexOf('USING');
      const checkIndex = rest.indexOf('WITH');
      const policyUsing = usingIndex !== -1 ? rest.slice(usingIndex + 1, checkIndex !== -1 ? checkIndex : undefined).join(' ') : '';
      const policyCheck = checkIndex !== -1 ? rest.slice(checkIndex + 2).join(' ') : '';

      sql += `DROP POLICY IF EXISTS ${policyName} ON ${table};\n`;
      sql += `CREATE POLICY ${policyName} ON ${table} FOR ${policyAction} TO ${policyType} `;
      if (policyUsing) sql += `USING (${policyUsing}) `;
      if (policyCheck) sql += `WITH CHECK (${policyCheck})`;
      sql += ';\n';
    });
    return sql;
  };

  // Helper function to handle permissions
  const handlePermissions = (table: string, columns: { [key: string]: ColumnInfo }): string => {
    const permissions: { [key: string]: { [key: string]: Set<string> } } = {};

    Object.entries(columns).forEach(([colName, colInfo]) => {
      colInfo.permissions.forEach(permission => {
        const [privilege, _, __, grantee] = permission.split(' ');
        if (!permissions[grantee]) permissions[grantee] = {};
        if (!permissions[grantee][privilege]) permissions[grantee][privilege] = new Set();
        permissions[grantee][privilege].add(colName);
      });
    });

    let sql = '';
    Object.entries(permissions).forEach(([grantee, privileges]) => {
      const grantStatements = Object.entries(privileges).map(([privilege, columns]) =>
        `${privilege} (${Array.from(columns).join(', ')})`
      );
      if (grantStatements.length > 0) {
        sql += `${grantStatements.join(', ')} ON ${table} TO ${grantee};\n`;
      }
    });

    return sql;
  };

  // Drop removed tables
  diff.tablesRemoved.forEach(table => {
    psql += `DROP TABLE IF EXISTS ${table} CASCADE;\n`;
  });

  // Create new tables and alter existing ones
  [...diff.tablesAdded, ...Object.keys(diff.tablesDiff)].forEach(table => {
    const tableSchema = fullSchema[table];
    const isNewTable = diff.tablesAdded.includes(table);

    if (isNewTable) {
      // Create new table
      const columns = Object.entries(tableSchema.columns)
        .map(([colName, colInfo]) => getColumnDefinitionSQL(colName, colInfo))
        .join(', ');
      psql += `CREATE TABLE ${table} (${columns});\n`;

      // Add primary key constraint
      const primaryKeys = Object.entries(tableSchema.columns)
        .filter(([_, colInfo]) => colInfo.isPrimaryKey)
        .map(([colName, _]) => colName);
      if (primaryKeys.length > 0) {
        psql += `ALTER TABLE ${table} ADD CONSTRAINT ${table}_pkey PRIMARY KEY (${primaryKeys.join(', ')});\n`;
      }

      // Add foreign key constraints
      tableSchema.foreignKeys.forEach(fk => {
        psql += `ALTER TABLE ${table} ADD CONSTRAINT ${table}_${fk.columnName}_fkey ` +
          `FOREIGN KEY (${fk.columnName}) REFERENCES ${fk.referenceTable}(${fk.referenceColumn}) ` +
          `ON UPDATE ${fk.updateRule} ON DELETE ${fk.deleteRule};\n`;
      });
    } else {
      // Alter existing table
      const tableDiff = diff.tablesDiff[table];

      // Add new columns
      tableDiff.columnsAdded.forEach(col => {
        psql += `ALTER TABLE ${table} ADD COLUMN ${getColumnDefinitionSQL(col, tableSchema.columns[col])};\n`;
      });

      // Remove deleted columns
      tableDiff.columnsRemoved.forEach(col => {
        psql += `ALTER TABLE ${table} DROP COLUMN IF EXISTS ${col} CASCADE;\n`;
      });

      // Modify changed columns
      Object.entries(tableDiff.columnsDiff).forEach(([col, colDiff]) => {
        if (colDiff.from.type !== colDiff.to.type || colDiff.from.maxLength !== colDiff.to.maxLength) {
          psql += `ALTER TABLE ${table} ALTER COLUMN ${col} TYPE ${getColumnDefinition(colDiff.to)} USING ${col}::${getColumnDefinition(colDiff.to)};\n`;
        }
        if (colDiff.from.isNullable !== colDiff.to.isNullable) {
          psql += `ALTER TABLE ${table} ALTER COLUMN ${col} ${colDiff.to.isNullable ? 'DROP' : 'SET'} NOT NULL;\n`;
        }
        if (colDiff.from.defaultValue !== colDiff.to.defaultValue) {
          psql += colDiff.to.defaultValue
            ? `ALTER TABLE ${table} ALTER COLUMN ${col} SET DEFAULT ${colDiff.to.defaultValue};\n`
            : `ALTER TABLE ${table} ALTER COLUMN ${col} DROP DEFAULT;\n`;
        }
      });

      // Update foreign key constraints
      if (tableDiff.foreignKeys) {
        tableSchema.foreignKeys.forEach(fk => {
          psql += `ALTER TABLE ${table} DROP CONSTRAINT IF EXISTS ${table}_${fk.columnName}_fkey;\n`;
          psql += `ALTER TABLE ${table} ADD CONSTRAINT ${table}_${fk.columnName}_fkey ` +
            `FOREIGN KEY (${fk.columnName}) REFERENCES ${fk.referenceTable}(${fk.referenceColumn}) ` +
            `ON UPDATE ${fk.updateRule} ON DELETE ${fk.deleteRule};\n`;
        });
      }
    }

    // Handle RLS policies and permissions for all tables
    psql += handleRLSPolicies(table, tableSchema.rlsPolicies);
    psql += handlePermissions(table, tableSchema.columns);
  });

  return psql;
}
