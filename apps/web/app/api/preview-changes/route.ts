import { createClient } from '@/utils/supabase/server'
import { previewChanges } from '@db-schema-diff-preview/db-utils'
import { NextRequest, NextResponse } from 'next/server'

export async function POST(request: NextRequest) {
    const supabase = createClient()

    const { data: { user } } = await supabase.auth.getUser()

    if (!user) {
        return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { connectionString1, connectionString2, psql } = await request.json()

    try {
        const preview = await previewChanges(user.id, connectionString1, connectionString2, psql)
        
        if (preview.verificationResult && !preview.verificationResult.isValid) {
            return NextResponse.json({
                error: 'Invalid PSQL',
                details: preview.verificationResult.errors
            }, { status: 400 })
        }
        
        return NextResponse.json(preview)
    } catch (error) {
        console.error('Error previewing changes:', error)
        return NextResponse.json({
            error: 'Failed to preview changes',
            details: error instanceof Error ? error.message : 'Unknown error'
        }, { status: 500 })
    }
}