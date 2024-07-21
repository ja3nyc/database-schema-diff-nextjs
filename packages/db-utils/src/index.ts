
import Docker from 'dockerode';
import { Pool } from 'pg';
import { v4 as uuidv4 } from 'uuid';

const docker = new Docker();

interface ColumnInfo {
    type: string;
    maxLength: number | null;
    isNullable: boolean;
    defaultValue: string | null;
    isPrimaryKey: boolean;
}

interface ForeignKeyInfo {
    columnName: string;
    referenceTable: string;
    referenceColumn: string;
    updateRule: string;
    deleteRule: string;
}

interface TableInfo {
    columns: { [columnName: string]: ColumnInfo };
    foreignKeys: ForeignKeyInfo[];
}

interface DatabaseSchema {
    [tableName: string]: TableInfo;
}

interface SchemaDiff {
    tablesAdded: string[];
    tablesRemoved: string[];
    tablesDiff: {
        [tableName: string]: {
            columnsAdded: string[];
            columnsRemoved: string[];
            columnsDiff: {
                [columnName: string]: {
                    from: ColumnInfo;
                    to: ColumnInfo;
                };
            };
            foreignKeysAdded: ForeignKeyInfo[];
            foreignKeysRemoved: ForeignKeyInfo[];
        };
    };
}

interface PreviewContainer {
    id: string;
    connectionString: string;
    lastAccessed: number;
}

const activeContainers: { [userId: string]: PreviewContainer } = {};

async function startPreviewContainer(userId: string): Promise<string> {
    const containerId = `preview-${userId}-${uuidv4()}`;
    const containerPort = 5433 + Object.keys(activeContainers).length; // Assuming we start from 5433

    await docker.pull('postgres:13');

    const container = await docker.createContainer({
        Image: 'postgres:13',
        name: containerId,
        Env: [
            'POSTGRES_DB=preview_db',
            'POSTGRES_USER=preview_user',
            'POSTGRES_PASSWORD=preview_password'
        ],
        HostConfig: {
            PortBindings: { '5432/tcp': [{ HostPort: containerPort.toString() }] }
        }
    });

    await container.start();

    // Wait for Postgres to be ready
    await new Promise(resolve => setTimeout(resolve, 5000));

    const connectionString = `postgresql://preview_user:preview_password@localhost:${containerPort}/preview_db`;

    activeContainers[userId] = {
        id: containerId,
        connectionString,
        lastAccessed: Date.now()
    };

    return connectionString;
}

async function stopPreviewContainer(userId: string) {
    const containerInfo = activeContainers[userId];
    if (containerInfo) {
        const container = docker.getContainer(containerInfo.id);
        await container.stop();
        await container.remove();
        delete activeContainers[userId];
    }
}

async function cleanupInactiveContainers() {
    const now = Date.now();
    const inactiveThreshold = 30 * 60 * 1000; // 30 minutes

    for (const [userId, containerInfo] of Object.entries(activeContainers)) {
        if (now - containerInfo.lastAccessed > inactiveThreshold) {
            await stopPreviewContainer(userId);
        }
    }
}

async function getSchema(connectionString: string): Promise<DatabaseSchema> {
    const pool = new Pool({ connectionString });
    try {
        const tablesResult = await pool.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
    `);

        const schema: DatabaseSchema = {};

        for (const { table_name } of tablesResult.rows) {
            const columnsResult = await pool.query(`
        SELECT column_name, data_type, character_maximum_length, is_nullable, column_default,
               (SELECT true FROM information_schema.key_column_usage
                WHERE table_name = c.table_name AND column_name = c.column_name
                  AND constraint_name = (SELECT constraint_name FROM information_schema.table_constraints
                                         WHERE table_name = c.table_name AND constraint_type = 'PRIMARY KEY'
                                         LIMIT 1)) as is_primary_key
        FROM information_schema.columns c
        WHERE table_name = $1
      `, [table_name]);

            const foreignKeysResult = await pool.query(`
        SELECT
          kcu.column_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name,
          rc.update_rule,
          rc.delete_rule
        FROM information_schema.key_column_usage AS kcu
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = kcu.constraint_name
        JOIN information_schema.referential_constraints AS rc
          ON rc.constraint_name = kcu.constraint_name
        WHERE kcu.table_name = $1
      `, [table_name]);

            schema[table_name] = {
                columns: {},
                foreignKeys: foreignKeysResult.rows.map(row => ({
                    columnName: row.column_name,
                    referenceTable: row.foreign_table_name,
                    referenceColumn: row.foreign_column_name,
                    updateRule: row.update_rule,
                    deleteRule: row.delete_rule
                }))
            };

            for (const column of columnsResult.rows) {
                schema[table_name].columns[column.column_name] = {
                    type: column.data_type,
                    maxLength: column.character_maximum_length,
                    isNullable: column.is_nullable === 'YES',
                    defaultValue: column.column_default,
                    isPrimaryKey: column.is_primary_key || false
                };
            }
        }

        return schema;
    } finally {
        await pool.end();
    }
}

function compareSchemas(schema1: DatabaseSchema, schema2: DatabaseSchema): SchemaDiff {
    const diff: SchemaDiff = {
        tablesAdded: [],
        tablesRemoved: [],
        tablesDiff: {}
    };

    // Find added and removed tables
    diff.tablesAdded = Object.keys(schema2).filter(table => !(table in schema1));
    diff.tablesRemoved = Object.keys(schema1).filter(table => !(table in schema2));

    // Compare tables that exist in both schemas
    for (const table of Object.keys(schema1)) {
        if (table in schema2) {
            const tableDiff: SchemaDiff['tablesDiff'][string] = {
                columnsAdded: [],
                columnsRemoved: [],
                columnsDiff: {},
                foreignKeysAdded: [],
                foreignKeysRemoved: []
            };

            // Compare columns
            for (const column of Object.keys(schema1[table].columns)) {
                if (!(column in schema2[table].columns)) {
                    tableDiff.columnsRemoved.push(column);
                } else if (JSON.stringify(schema1[table].columns[column]) !== JSON.stringify(schema2[table].columns[column])) {
                    tableDiff.columnsDiff[column] = {
                        from: schema1[table].columns[column],
                        to: schema2[table].columns[column]
                    };
                }
            }
            for (const column of Object.keys(schema2[table].columns)) {
                if (!(column in schema1[table].columns)) {
                    tableDiff.columnsAdded.push(column);
                }
            }

            // Compare foreign keys
            const fk1 = schema1[table].foreignKeys;
            const fk2 = schema2[table].foreignKeys;
            tableDiff.foreignKeysAdded = fk2.filter(fk => !fk1.some(f => JSON.stringify(f) === JSON.stringify(fk)));
            tableDiff.foreignKeysRemoved = fk1.filter(fk => !fk2.some(f => JSON.stringify(f) === JSON.stringify(fk)));

            // Check if there are any differences
            if (
                tableDiff.columnsAdded.length > 0 ||
                tableDiff.columnsRemoved.length > 0 ||
                Object.keys(tableDiff.columnsDiff).length > 0 ||
                tableDiff.foreignKeysAdded.length > 0 ||
                tableDiff.foreignKeysRemoved.length > 0
            ) {
                diff.tablesDiff[table] = tableDiff;
            }
        }
    }

    return diff;
}

function generatePsql(diff: SchemaDiff): string {
    let psql = '';

    // Drop removed tables
    for (const table of diff.tablesRemoved) {
        psql += `DROP TABLE IF EXISTS "${table}" CASCADE;\n`;
    }

    // Create new tables
    for (const table of diff.tablesAdded) {
        psql += `CREATE TABLE "${table}" ();\n`;
    }

    // Alter existing tables
    for (const [table, tableDiff] of Object.entries(diff.tablesDiff)) {
        // Add new columns
        for (const column of tableDiff.columnsAdded) {
            const columnInfo = tableDiff.columnsDiff[column].to;
            psql += `ALTER TABLE "${table}" ADD COLUMN "${column}" ${columnInfo.type}`;
            if (columnInfo.maxLength) psql += `(${columnInfo.maxLength})`;
            if (!columnInfo.isNullable) psql += ' NOT NULL';
            if (columnInfo.defaultValue) psql += ` DEFAULT ${columnInfo.defaultValue}`;
            psql += ';\n';
        }

        // Modify changed columns
        for (const [column, columnDiff] of Object.entries(tableDiff.columnsDiff)) {
            if (columnDiff.from.type !== columnDiff.to.type || columnDiff.from.maxLength !== columnDiff.to.maxLength) {
                psql += `ALTER TABLE "${table}" ALTER COLUMN "${column}" TYPE ${columnDiff.to.type}`;
                if (columnDiff.to.maxLength) psql += `(${columnDiff.to.maxLength})`;
                psql += ';\n';
            }
            if (columnDiff.from.isNullable !== columnDiff.to.isNullable) {
                psql += `ALTER TABLE "${table}" ALTER COLUMN "${column}" ${columnDiff.to.isNullable ? 'DROP' : 'SET'} NOT NULL;\n`;
            }
            if (columnDiff.from.defaultValue !== columnDiff.to.defaultValue) {
                if (columnDiff.to.defaultValue) {
                    psql += `ALTER TABLE "${table}" ALTER COLUMN "${column}" SET DEFAULT ${columnDiff.to.defaultValue};\n`;
                } else {
                    psql += `ALTER TABLE "${table}" ALTER COLUMN "${column}" DROP DEFAULT;\n`;
                }
            }
        }

        // Remove deleted columns
        for (const column of tableDiff.columnsRemoved) {
            psql += `ALTER TABLE "${table}" DROP COLUMN "${column}";\n`;
        }

        // Add new foreign keys
        for (const fk of tableDiff.foreignKeysAdded) {
            psql += `ALTER TABLE "${table}" ADD CONSTRAINT "${table}_${fk.columnName}_fkey" ` +
                `FOREIGN KEY ("${fk.columnName}") REFERENCES "${fk.referenceTable}" ("${fk.referenceColumn}") ` +
                `ON UPDATE ${fk.updateRule} ON DELETE ${fk.deleteRule};\n`;
        }

        // Remove deleted foreign keys
        for (const fk of tableDiff.foreignKeysRemoved) {
            psql += `ALTER TABLE "${table}" DROP CONSTRAINT "${table}_${fk.columnName}_fkey";\n`;
        }
    }

    return psql;
}

async function applyPsqlToPreviewDB(connectionString: string, psql: string): Promise<void> {
    const pool = new Pool({ connectionString });
    try {
        await pool.query(psql);
    } finally {
        await pool.end();
    }
}

export async function previewChanges(userId: string, sourceConnectionString: string, targetConnectionString: string): Promise<{ psql: string; diff: SchemaDiff }> {
    let previewConnectionString: string;

    if (activeContainers[userId]) {
        previewConnectionString = activeContainers[userId].connectionString;
        activeContainers[userId].lastAccessed = Date.now();
    } else {
        previewConnectionString = await startPreviewContainer(userId);
    }

    try {
        // Copy the source schema to the preview database
        const sourceSchema = await getSchema(sourceConnectionString);
        const createSchemaSQL = generatePsql({
            tablesAdded: Object.keys(sourceSchema),
            tablesRemoved: [],
            tablesDiff: {}
        });
        await applyPsqlToPreviewDB(previewConnectionString, createSchemaSQL);

        // Generate PSQL for changes
        const targetSchema = await getSchema(targetConnectionString);
        const diff = compareSchemas(sourceSchema, targetSchema);
        const psql = generatePsql(diff);

        // Apply PSQL to preview database
        await applyPsqlToPreviewDB(previewConnectionString, psql);

        // Compare preview database with target
        const previewSchema = await getSchema(previewConnectionString);
        const finalDiff = compareSchemas(previewSchema, targetSchema);

        return { psql, diff: finalDiff };
    } catch (error) {
        // If there's an error, stop the container to free up resources
        await stopPreviewContainer(userId);
        throw error;
    }
}

// Run cleanup every 5 minutes
setInterval(cleanupInactiveContainers, 5 * 60 * 1000);

export type { DatabaseSchema, SchemaDiff };

