<template>
  <div class="flex w-screen h-screen justify-center items-center bg-white">
    <div class="grid grid-cols-12 w-full max-w-[1376px] gap-6 items-center">
      <div class="col-span-8">
        <img src="../assets/illustration.svg" alt="illustration" />
      </div>
      <div class="col-span-4 flex flex-col items-start gap-8">
        <div class="self-stretch text-black text-6xl font-bold font-['Figtree'] leading-[normal]">
          Welcome to <br />AdventureWork
        </div>
        <div class="flex flex-col items-start gap-8 self-stretch">
          <div class="flex flex-col items-start gap-2.5 self-stretch">
            <div class="self-stretch text-black [font-family:Figtree] text-base font-normal leading-6">
              Account
            </div>
            <input
              class="w-full flex items-center gap-2.5 flex-[1_0_0] border border-border bg-white [font-family:Figtree] px-6 py-3 rounded-lg border-solid"
              type="text" v-model="account" placeholder="Enter your account" />
          </div>
        </div>
        <div class="flex flex-col items-start gap-8 self-stretch">
          <div class="flex flex-col items-start gap-2.5 self-stretch">
            <div class="self-stretch text-black [font-family:Figtree] text-base font-normal leading-6">
              Password
            </div>
            <input
              class="w-full flex items-center gap-2.5 flex-[1_0_0] border border-border bg-white [font-family:Figtree] px-6 py-3 rounded-lg border-solid"
              type="password" v-model="password" placeholder="Enter your password" />
          </div>
        </div>
        <div v-if="errorMessage" class="text-red-500 font-medium">{{ errorMessage }}</div>
        <button @click="login"
          class="flex justify-center items-center gap-2.5 self-stretch bg-primary px-6 py-3 rounded-xl text-white [font-family:Figtree] text-base font-bold leading-6">
          Login
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from "vue";
import axios from 'axios';  // Import axios
import router from "../router"; // make sure you import router if you need to navigate after login
import "../assets/styles/tailwind.css";

const account = ref("");
const password = ref("");
const errorMessage = ref("");
const loginURL = "http://0.0.0.0:8081/authentication/login";

async function login() {
  errorMessage.value = "";
  const formData = new FormData();
  formData.append('username', account.value);
  formData.append('password', password.value);

  try {
    const response = await axios.post(loginURL, formData);
    if (response.data.access_token) {
      localStorage.setItem('access_token', response.data.access_token);

      axios.defaults.headers.common['Authorization'] = `Bearer ${response.data.access_token}`;

      router.push('/dashboard');
    } else {
      throw new Error('No access token received');
    }
  } catch (error) {
    // console.error('Login error:', error);
    // alert('Login failed: ' + (error.response?.data?.message || error.message));
    errorMessage.value = error.response?.data?.message || 'Incorrect username or password';
  }
}
</script>
