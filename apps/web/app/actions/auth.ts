"use server";

import { createClient } from "@/utils/supabase/server";

export const signIn = async (
	formData: FormData,
): Promise<{
	status: number;
	message: string;
}> => {
	return new Promise(async (resolve, reject) => {

		const email = formData.get("email") as string;
		const password = formData.get("password") as string;
		const supabase = createClient();

		const { error } = await supabase.auth.signInWithPassword({
			email,
			password,
		});
		console.log("error", error);
		if (error) {
			return resolve({
				status: 500,
				message: `Error: ${error.message}`,
			});
		}

		return resolve({
			status: 200,
			message: "User authenticated. Redirecting to dashboard.",
		});
	});
};
