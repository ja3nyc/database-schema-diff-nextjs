"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { signIn } from "../actions/auth";

export default function Login() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const router = useRouter();

  const handleSignIn = async (formData: FormData) => {
    signIn(formData).then(() => {
      router.push("/");
    });
  };

  return (
    <form action={handleSignIn}>
      <div className="max-w-xl mx-auto p-4">
        <div className="text-3xl font-bold mb-6">Sign In</div>
        <Label>Email</Label>
        <Input
          name="email"
          onChange={(e) => setEmail(e.target.value)}
          value={email}
        />
        <Label>Password</Label>
        <Input
          type="password"
          name="password"
          onChange={(e) => setPassword(e.target.value)}
          value={password}
        />
        <div className="mt-4 flex justify-between">
          <Button type="submit">Sign In</Button>
        </div>
      </div>
    </form>
  );
}
