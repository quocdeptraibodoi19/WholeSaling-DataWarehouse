<template>
  <nav class="py-4 bg-background-accent flex flex-row justify-center sticky top-0 z-10">
    <div class="flex flex-row justify-between justify-center max-w-custom w-full">
      <div class="flex items-center gap-12">
        <div class="font-bold text-primary text-2xl [font-family:Figtree]">
          AdventureWork
        </div>
        <router-link to="/dashboard"
          class="nav-link text-text text-xl font-normal [font-family:Figtree]">Home</router-link>
      </div>
      <button @click="logout"
        class="inline-flex justify-center items-center gap-2.5 px-6 py-3 rounded-xl text-primary font-bold [font-family:Figtree]">
        Log Out
      </button>
    </div>
  </nav>
</template>

<script setup>
import { useRouter } from "vue-router";
const router = useRouter();

function logout() {
  console.log(router);
  sessionStorage.clear(); // Clearing session storage or any authentication tokens.
  router.push("/login"); // Navigate to login page.
}
</script>

<style scoped>
.nav-link {
  text-decoration: none;
}
</style>
