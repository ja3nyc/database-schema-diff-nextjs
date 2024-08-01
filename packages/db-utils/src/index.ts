
import { Pool } from 'pg';
import { newDb } from 'pg-mem';
import { parse } from 'pgsql-ast-parser';
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

function verifyPsql(psql: string): { isValid: boolean; errors: string[] } {
    const errors: string[] = [];
    const statements = psql.split(';').filter(stmt => stmt.trim() !== '');

    for (const statement of statements) {
        try {
            parse(statement);
        } catch (error: any) {
            errors.push(`Error in statement: ${statement.trim()}\nError details: ${error.message}`);
        }
    }

    // Additional checks
    if (psql.toLowerCase().includes('drop database') || psql.toLowerCase().includes('drop schema')) {
        errors.push('DROP DATABASE or DROP SCHEMA statements are not allowed for safety reasons.');
    }

    if (psql.toLowerCase().includes('truncate')) {
        errors.push('TRUNCATE statements are not recommended for this preview.');
    }

    return {
        isValid: errors.length === 0,
        errors
    };
}

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

    let tables;
    if (db instanceof Pool) {
        // This is a real PostgreSQL database
        const result = await db.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
      `);
        tables = result.rows.map(row => row.table_name);
    } else {
        // This is a pg-mem database
        tables = db.public.tables.map((table: any) => table.name);
    }

    for (const tableName of tables) {
        schema[tableName] = {
            columns: {},
            foreignKeys: []
        };

        let columns, foreignKeys;

        if (db instanceof Pool) {
            // Real PostgreSQL database
            columns = (await db.query(`
          SELECT 
            c.column_name, 
            c.data_type, 
            c.is_nullable, 
            c.column_default,
            CASE WHEN pk.constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_primary_key
          FROM information_schema.columns c
          LEFT JOIN (
            SELECT kcu.column_name, tc.constraint_type
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
          ) pk ON pk.column_name = c.column_name
          WHERE c.table_name = $1
        `, [tableName])).rows;

            foreignKeys = (await db.query(`
          SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
          FROM information_schema.key_column_usage AS kcu
          JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = kcu.constraint_name
          JOIN information_schema.table_constraints AS tc
            ON tc.constraint_name = kcu.constraint_name
          WHERE kcu.table_name = $1
            AND tc.constraint_type = 'FOREIGN KEY'
        `, [tableName])).rows;
        } else {
            // pg-mem database
            const table = db.public.tables.find((t: any) => t.name === tableName);
            columns = Object.values(table.columns);
            foreignKeys = table.foreignKeys;
        }

        for (const column of columns) {
            schema[tableName].columns[column.column_name || column.name] = {
                type: column.data_type || column.type,
                maxLength: null, // Not easily available in both cases
                isNullable: column.is_nullable === 'YES' || !column.notnull,
                defaultValue: column.column_default || column.default_value,
                isPrimaryKey: column.is_primary_key || column.isPrimaryKey || false
            };
        }

        for (const fk of foreignKeys) {
            schema[tableName].foreignKeys.push({
                columnName: fk.column_name || fk.columns[0],
                referenceTable: fk.foreign_table_name || fk.referencedTable,
                referenceColumn: fk.foreign_column_name || fk.referencedColumns[0],
                updateRule: 'NO ACTION', // Not easily available in both cases
                deleteRule: 'NO ACTION' // Not easily available in both cases
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


async function applyPsqlToPreviewDB(db: any, psql: string): Promise<void> {
    const statements = psql.split(';').filter(stmt => stmt.trim() !== '');
    for (const statement of statements) {
        try {
            if (db instanceof Pool) {
                // Real PostgreSQL database
                await db.query(statement);
            } else {
                // pg-mem database
                db.public.none(statement);
            }
        } catch (error) {
            console.error(`Error executing statement: ${statement}`);
            console.error(error);
        }
    }
}

export async function previewChanges(userId: string, sourceConnectionString: string, targetConnectionString: string, psql: string): Promise<{ diff: SchemaDiff; verificationResult?: { isValid: boolean; errors: string[] } }> {
    let previewDb: any;

    if (activeDatabases[userId]) {
        previewDb = activeDatabases[userId].db;
        activeDatabases[userId].lastAccessed = Date.now();
    } else {
        previewDb = await createPreviewDatabase(userId);
    }

    // Verify PSQL before proceeding
    const verificationResult = verifyPsql(psql);
    if (!verificationResult.isValid) {
        return {
            diff: {
                tablesAdded: [],
                tablesRemoved: [],
                tablesDiff: {}
            }, verificationResult
        };
    }

    try {
        // Get source schema
        const sourcePool = new Pool({ connectionString: sourceConnectionString });
        const sourceSchema = await getSchema(sourcePool);
        await sourcePool.end();

        // Apply the provided PSQL to the preview database
        await applyPsqlToPreviewDB(previewDb, psql);

        // Get target schema
        const targetPool = new Pool({ connectionString: targetConnectionString });
        const targetSchema = await getSchema(targetPool);
        await targetPool.end();

        // Compare preview database with target
        const previewSchema = await getSchema(previewDb);
        const finalDiff = compareSchemas(previewSchema, targetSchema);

        return { diff: finalDiff, verificationResult };
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

