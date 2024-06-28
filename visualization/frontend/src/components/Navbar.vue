<template>
  <nav class="py-4 bg-background-accent flex flex-row justify-center sticky top-0 z-10">
    <div class="flex flex-row justify-between justify-center max-w-custom w-full">
      <div class="flex items-center gap-12">
        <router-link to="/dashboard" class="font-bold text-primary text-2xl [font-family:Figtree]">
          AdventureWork
        </router-link>
        <router-link to="/dashboard"
          class="nav-link text-text text-xl font-normal [font-family:Figtree]">Dashboard</router-link>
      </div>
      <button @click="logout"
        class="inline-flex justify-center items-center gap-2.5 px-6 py-3 rounded-xl text-primary font-bold [font-family:Figtree]">
        Log Out
      </button>
    </div>
  </nav>
</template>

<script setup>
import axios from 'axios';
import { useRouter } from "vue-router";
const router = useRouter();

function logout() {
  // Clear localStorage where the token is stored
  localStorage.removeItem('access_token');

  // Optionally, if you use axios defaults for tokens, remove it
  if (axios.defaults.headers.common['Authorization']) {
    delete axios.defaults.headers.common['Authorization'];
  }

  // Use sessionStorage.clear() if you are storing other session-related data
  sessionStorage.clear(); // Clearing session storage or any authentication tokens.

  // Navigate to login page
  router.push("/login").catch(error => {
    console.error("Router error:", error);
  });
}
</script>

<style scoped>
.nav-link {
  text-decoration: none;
}
</style>
