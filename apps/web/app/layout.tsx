import { createClient } from '@/utils/supabase/server';
import './globals.css'

export const metadata = {
  title: 'Schema Diff App',
  description: 'Compare and analyze database schemas',
}

export default async function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const supabase = createClient();

  const {
    data: { user },
  } = await supabase.auth.getUser();

  return (
    <html lang="en">
      <body>
        <main>{children}</main>
      </body>
    </html>
  )
}