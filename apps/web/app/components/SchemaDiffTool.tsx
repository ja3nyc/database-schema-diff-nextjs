"use client";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useSupabaseBrowser } from "@/utils/supabase/client";
import { ArrowRightIcon, CheckIcon, CopyIcon, EyeIcon } from "lucide-react";
import { useState } from "react";

interface SchemaDiff {
  tablesAdded: string[];
  tablesRemoved: string[];
  tablesDiff: {
    [tableName: string]: {
      columnsAdded: string[];
      columnsRemoved: string[];
      columnsDiff: {
        [columnName: string]: {
          from: { type: string; maxLength: number | null };
          to: { type: string; maxLength: number | null };
        };
      };
    };
  };
}
interface SchemaDiffToolProps {
  userId: string;
}
export default function SchemaDiffTool({ userId }: SchemaDiffToolProps) {
  const [connectionString1, setConnectionString1] = useState<string>("");
  const [connectionString2, setConnectionString2] = useState<string>("");
  const [schemaDiff, setSchemaDiff] = useState<SchemaDiff | null>(null);
  const [psqlCommands, setPsqlCommands] = useState<string>("");
  const [isSchemaCollapsed, setIsSchemaCollapsed] = useState(true);
  const [isPsqlCollapsed, setIsPsqlCollapsed] = useState(true);
  const [copied, setCopied] = useState(false);
  const [previewDiff, setPreviewDiff] = useState<SchemaDiff | null>(null);
  const [previewPsql, setPreviewPsql] = useState<string>("");

  const supabase = useSupabaseBrowser();

  const handleCompare = async () => {
    try {
      const response = await fetch("/api/compare-schemas", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ connectionString1, connectionString2 }),
      });

      if (!response.ok) {
        throw new Error("Failed to compare schemas");
      }

      const result = await response.json();
      setSchemaDiff(result.diff);
      setPsqlCommands(result.psql);
    } catch (error) {
      console.error("Error comparing schemas:", error);
      alert(
        "Error comparing schemas. Please check your connection strings and try again."
      );
    }
  };

  const handleCopy = async (text: string) => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const ToggleButton = ({
    isCollapsed,
    setIsCollapsed,
    text,
  }: {
    isCollapsed: boolean;
    setIsCollapsed: (value: boolean) => void;
    text: string;
  }) => (
    <Button onClick={() => setIsCollapsed(!isCollapsed)} variant="outline">
      {isCollapsed ? `Show ${text}` : `Hide ${text}`}
    </Button>
  );

  const handlePreview = async () => {
    try {
      const response = await fetch("/api/preview-changes", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ connectionString1, connectionString2 }),
      });

      if (!response.ok) {
        throw new Error("Failed to preview changes");
      }

      const result = await response.json();
      setPreviewPsql(result.psql);
      setPreviewDiff(result.diff);
    } catch (error) {
      console.error("Error previewing changes:", error);
      alert("Error previewing changes. Please try again.");
    }
  };

  return (
    <div>
      <Dialog>
        <DialogTrigger asChild>
          <Button onClick={handlePreview} variant="outline">
            <EyeIcon className="mr-2 h-4 w-4" />
            Preview Changes
          </Button>
        </DialogTrigger>
        <DialogContent className="max-w-4xl">
          <DialogHeader>
            <DialogTitle>Change Preview</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <h3 className="text-lg font-semibold">PSQL to be executed:</h3>
              <pre className="bg-slate-100 p-4 rounded mt-2 overflow-auto max-h-[30vh]">
                {previewPsql}
              </pre>
            </div>
            <div>
              <h3 className="text-lg font-semibold">Resulting differences:</h3>
              <pre className="bg-slate-100 p-4 rounded mt-2 overflow-auto max-h-[30vh]">
                {JSON.stringify(previewDiff, null, 2)}
              </pre>
            </div>
          </div>
        </DialogContent>
      </Dialog>
      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Compare Database Schemas</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-6">
            <div className="space-y-2">
              <Label htmlFor="connection1">Source Database</Label>
              <Input
                id="connection1"
                placeholder="Enter connection string for the source database"
                value={connectionString1}
                onChange={(e) => setConnectionString1(e.target.value)}
              />
            </div>
            <div className="flex justify-center items-center">
              <ArrowRightIcon className="h-6 w-6" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="connection2">Target Database</Label>
              <Input
                id="connection2"
                placeholder="Enter connection string for the target database"
                value={connectionString2}
                onChange={(e) => setConnectionString2(e.target.value)}
              />
            </div>
            <Button onClick={handleCompare} className="w-full">
              Compare Schemas
            </Button>
          </div>
        </CardContent>
      </Card>
      {schemaDiff && (
        <Card className="mb-6">
          <CardHeader>
            <CardTitle>Schema Differences</CardTitle>
          </CardHeader>
          <CardContent>
            <ToggleButton
              isCollapsed={isSchemaCollapsed}
              setIsCollapsed={setIsSchemaCollapsed}
              text="Schema Differences"
            />
            {!isSchemaCollapsed && (
              <pre className="bg-slate-100 p-4 rounded mt-2 overflow-auto max-h-96">
                {JSON.stringify(schemaDiff, null, 2)}
              </pre>
            )}
          </CardContent>
        </Card>
      )}
      {psqlCommands && (
        <Card className="mb-6">
          <CardHeader>
            <CardTitle>PSQL Commands to Update Target Database</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex justify-between items-center mb-2">
              <ToggleButton
                isCollapsed={isPsqlCollapsed}
                setIsCollapsed={setIsPsqlCollapsed}
                text="PSQL Commands"
              />
              <Button
                onClick={() => handleCopy(psqlCommands)}
                variant="outline"
                size="icon"
              >
                {copied ? (
                  <CheckIcon className="h-4 w-4" />
                ) : (
                  <CopyIcon className="h-4 w-4" />
                )}
              </Button>
            </div>
            {!isPsqlCollapsed && (
              <pre className="bg-slate-100 p-4 rounded mt-2 overflow-auto max-h-96">
                {psqlCommands}
              </pre>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
