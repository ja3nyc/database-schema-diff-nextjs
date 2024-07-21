import { NextRequest, NextResponse } from 'next/server';
import { compareSchemas, generatePsql, getSchema } from '../../../utils/dbUtils';

export async function POST(request: NextRequest) {
    const { connectionString1, connectionString2 } = await request.json();

    try {
        const schema1 = await getSchema(connectionString1);
        const schema2 = await getSchema(connectionString2);
        const diff = await compareSchemas(schema1, schema2);
        const psql = generatePsql(diff, schema2); // Use schema2 as the target schema
        return NextResponse.json({ diff, psql });
    } catch (error) {
        console.error('Error comparing schemas:', error);
        return NextResponse.json({
            error: 'Failed to compare schemas',
            details: error instanceof Error ? error.message : 'Unknown error'
        }, { status: 500 });
    }
}