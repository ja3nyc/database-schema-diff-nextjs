
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import SchemaDiffTool from "./components/SchemaDiffTool";
import { useSupabaseBrowser } from "@/utils/supabase/client";
import { createClient } from "@/utils/supabase/server";

export default async function Home() {
  const supabase = createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    redirect("/login");
  }
  return (
    <div className="container mx-auto p-4">
      <h1 className="text-3xl font-bold mb-6">Postgres Schema Diff Tool</h1>
      <SchemaDiffTool userId={user.id} />
    </div>
  );
}
