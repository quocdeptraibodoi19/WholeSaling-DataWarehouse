<template>
  <div class="flex w-screen h-screen justify-center items-center bg-white">
    <div class="grid grid-cols-12 w-full max-w-[1376px] gap-6 items-center">
      <div class="col-span-8">
        <img src="../assets/illustration.svg" alt="illustration" />
      </div>
      <div class="col-span-4 flex flex-col items-start gap-8">
        <div
          class="self-stretch text-black text-6xl font-bold font-['Figtree'] leading-[normal]"
        >
          Welcome to <br />AdventureWork
        </div>
        <div class="flex flex-col items-start gap-8 self-stretch">
          <div class="flex flex-col items-start gap-2.5 self-stretch">
            <div
              class="self-stretch text-black [font-family:Figtree] text-base font-normal leading-6"
            >
              Account
            </div>
            <input
              class="w-full flex items-center gap-2.5 flex-[1_0_0] border border-border bg-white [font-family:Figtree] px-6 py-3 rounded-lg border-solid"
              type="text"
              v-model="account"
              placeholder="Enter your account"
            />
          </div>
        </div>
        <div class="flex flex-col items-start gap-8 self-stretch">
          <div class="flex flex-col items-start gap-2.5 self-stretch">
            <div
              class="self-stretch text-black [font-family:Figtree] text-base font-normal leading-6"
            >
              Password
            </div>
            <input
              class="w-full flex items-center gap-2.5 flex-[1_0_0] border border-border bg-white [font-family:Figtree] px-6 py-3 rounded-lg border-solid"
              type="password"
              v-model="password"
              placeholder="Enter your password"
            />
          </div>
        </div>
        <button
          @click="login"
          class="flex justify-center items-center gap-2.5 self-stretch bg-primary px-6 py-3 rounded-xl text-white [font-family:Figtree] text-base font-bold leading-6"
        >
          Login
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from "vue";
import router from "../router"; // make sure you import router if you need to navigate after login
import "../assets/styles/tailwind.css";

const account = ref("");
const password = ref("");

async function login() {
  // Simple front-end validation
  if (!account.value || !password.value) {
    alert("Please enter both account and password");
    return;
  }
  router.push("/dashboard");

  // try {
  //   const response = await fetch("/api/login", {
  //     method: "POST",
  //     headers: {
  //       "Content-Type": "application/json",
  //     },
  //     body: JSON.stringify({
  //       account: account.value,
  //       password: password.value,
  //     }),
  //   });

  //   if (!response.ok) {
  //     throw new Error("Failed to login");
  //   }

  //   const data = await response.json();

  //   // You might want to store the token in localStorage or handle it differently
  //   localStorage.setItem("token", data.token);

  //   // Redirect to dashboard or another route on successful login
  //   router.push("/dashboard");
  // } catch (error) {
  //   console.error(error);
  //   alert("Login failed: " + error.message);
  // }
}
</script>
