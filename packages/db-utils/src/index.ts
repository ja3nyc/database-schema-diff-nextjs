
import { Pool } from 'pg';
import { newDb } from 'pg-mem';
import { v4 as uuidv4 } from 'uuid';

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
            columnsDiff: { [columnName: string]: { from: ColumnInfo; to: ColumnInfo } };
            foreignKeysAdded: ForeignKeyInfo[];
            foreignKeysRemoved: ForeignKeyInfo[];
        };
    };
}

interface PreviewDatabase {
    id: string;
    db: any; // pg-mem database instance
    lastAccessed: number;
}

const activeDatabases: { [userId: string]: PreviewDatabase } = {};

async function createPreviewDatabase(userId: string): Promise<any> {
    const dbId = `preview-${userId}-${uuidv4()}`;
    const db = newDb();

    activeDatabases[userId] = {
        id: dbId,
        db: db,
        lastAccessed: Date.now()
    };

    return db;
}

async function removePreviewDatabase(userId: string) {
    delete activeDatabases[userId];
}

async function cleanupInactiveDatabases() {
    const now = Date.now();
    const inactiveThreshold = 30 * 60 * 1000; // 30 minutes

    for (const [userId, dbInfo] of Object.entries(activeDatabases)) {
        if (now - dbInfo.lastAccessed > inactiveThreshold) {
            await removePreviewDatabase(userId);
        }
    }
}

async function getSchema(db: any): Promise<DatabaseSchema> {
    const schema: DatabaseSchema = {};

    const tables = db.public.tables;
    for (const table of tables) {
        const tableName = table.name;
        schema[tableName] = {
            columns: {},
            foreignKeys: []
        };

        for (const column of table.columns) {
            schema[tableName].columns[column.name] = {
                type: column.type,
                maxLength: null, // pg-mem doesn't provide this information
                isNullable: !column.notnull,
                defaultValue: column.default_value,
                isPrimaryKey: column.isPrimaryKey
            };
        }

        for (const fk of table.foreignKeys) {
            schema[tableName].foreignKeys.push({
                columnName: fk.columns[0],
                referenceTable: fk.referencedTable,
                referenceColumn: fk.referencedColumns[0],
                updateRule: 'NO ACTION', // pg-mem doesn't provide this information
                deleteRule: 'NO ACTION' // pg-mem doesn't provide this information
            });
        }
    }

    return schema;
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
            const tableDiff: {
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
            } = {
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
                diff.tablesDiff[table] = tableDiff as SchemaDiff['tablesDiff'][string];
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
async function applyPsqlToPreviewDB(db: any, psql: string): Promise<void> {
    db.public.query(psql);
}

export async function previewChanges(userId: string, sourceConnectionString: string, targetConnectionString: string): Promise<{ psql: string; diff: SchemaDiff }> {
    let previewDb: any;

    if (activeDatabases[userId]) {
        previewDb = activeDatabases[userId].db;
        activeDatabases[userId].lastAccessed = Date.now();
    } else {
        previewDb = await createPreviewDatabase(userId);
    }

    try {
        // Copy the source schema to the preview database
        const sourcePool = new Pool({ connectionString: sourceConnectionString });
        const sourceSchema = await getSchema(sourcePool);
        await sourcePool.end();

        const createSchemaSQL = generatePsql({
            tablesAdded: Object.keys(sourceSchema),
            tablesRemoved: [],
            tablesDiff: {}
        });
        await applyPsqlToPreviewDB(previewDb, createSchemaSQL);

        // Generate PSQL for changes
        const targetPool = new Pool({ connectionString: targetConnectionString });
        const targetSchema = await getSchema(targetPool);
        await targetPool.end();

        const diff = compareSchemas(sourceSchema, targetSchema);
        const psql = generatePsql(diff);

        // Apply PSQL to preview database
        await applyPsqlToPreviewDB(previewDb, psql);

        // Compare preview database with target
        const previewSchema = await getSchema(previewDb);
        const finalDiff = compareSchemas(previewSchema, targetSchema);

        return { psql, diff: finalDiff };
    } catch (error: any) {
        console.log('Error previewing changes:', error.message);
        // If there's an error, remove the database to free up resources
        await removePreviewDatabase(userId);
        throw error;
    }
}

// Run cleanup every 5 minutes
setInterval(cleanupInactiveDatabases, 5 * 60 * 1000);

export type { DatabaseSchema, SchemaDiff };

