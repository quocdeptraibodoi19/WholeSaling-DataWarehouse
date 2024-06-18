<template>
  <Navbar></Navbar>
  <div class="flex flex-col justify-center items-center gap-2.5 px-0 py-6">
    <div class="max-w-custom w-full flex flex-col items-start gap-8">
      <div class="flex justify-between items-center self-stretch">
        <div class="text-black [font-family:Figtree] text-5xl font-bold leading-[normal]">
          Sales Analysis
        </div>
        <div class="flex flex-row gap-3">
          <button @click="refreshData"
            class="flex justify-center items-center gap-2.5 self-stretch text-primary border-2 border-primary px-6 py-3 rounded-xl [font-family:Figtree] text-base font-bold leading-6">
            Refresh data
          </button>
          <router-link to="/newchart"
            class="inline-flex justify-center items-center gap-2.5 px-6 py-3 rounded-xl bg-primary [font-family:Figtree] text-white font-bold">Add
            new chart
          </router-link>

        </div>
      </div>



      <div v-if="totals" class="w-full h-24 justify-start items-start gap-6 inline-flex">
        <div
          class="grow shrink basis-0 px-6 py-3 bg-white rounded-lg border border-slate-200 flex-col justify-start items-start inline-flex">
          <div class="self-stretch text-text-secondary text-base font-semibold font-['Figtree'] leading-normal">
            Total Sales Amount
          </div>
          <div class="self-stretch text-sky-950 text-4xl font-bold font-['Figtree'] leading-normal">
            {{ totals.totalSalesAmount }}
          </div>
        </div>
        <div
          class="grow shrink basis-0 px-6 py-3 bg-white rounded-lg border border-slate-200 flex-col justify-start items-start inline-flex">
          <div class="self-stretch text-text-secondary text-base font-semibold font-['Figtree'] leading-normal">
            Total Number of Order
          </div>
          <div class="self-stretch text-sky-950 text-4xl font-bold font-['Figtree'] leading-normal">
            {{ totals.totalOrders }}
          </div>
        </div>
        <div
          class="grow shrink basis-0 px-6 py-3 bg-white rounded-lg border border-slate-200 flex-col justify-start items-start inline-flex">
          <div class="self-stretch text-text-secondary text-base font-semibold font-['Figtree'] leading-normal">
            Total Number of Customer
          </div>
          <div class="self-stretch text-sky-950 text-4xl font-bold font-['Figtree'] leading-normal">
            {{ totals.totalCustomers }}
          </div>
        </div>
        <div
          class="grow shrink basis-0 px-6 py-3 bg-white rounded-lg border border-slate-200 flex-col justify-start items-start inline-flex">
          <div class="self-stretch text-text-secondary text-base font-semibold font-['Figtree'] leading-normal">
            Total Number of Product
          </div>
          <div class="self-stretch text-sky-950 text-4xl font-bold font-['Figtree'] leading-normal">
            {{ totals.totalProducts }}
          </div>
        </div>
      </div>
      <div class="grid grid-cols-12 w-full gap-6">
        <Chart v-for="(chart, index) in charts" :key="chart.id" class="col-span-6" :chartType="chart.chartType"
          :chartData="chart.chart" :chartName="chart.chartName" :index="index" @deleteChart="deleteChart"></Chart>
        <router-link to="/newchart" v-if="isVisible"
          class="text-[#94a3b8] h-[400px] border border-solid rounded-2xl col-span-6 flex justify-center align-center items-center text-2xl [font-family:Figtree]">
          + New Chart
        </router-link>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="spinner-container flex flex-col justify-start items-center w-full">
        <div class="spinner  w-full"></div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.spinner {
  border: 4px solid rgba(0, 0, 0, 0.1);
  width: 128px;
  height: 128px;
  border-radius: 50%;
  border-left-color: #09f;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  0% {
    transform: rotate(0deg);
  }

  100% {
    transform: rotate(360deg);
  }
}

.spinner-container {
  margin-top: 128px;
}
</style>

<script setup>
import axios from 'axios';
import Highcharts from "highcharts";
import HighchartsVue from "vue-highcharts";
import mapInit from "highcharts/modules/map";
import worldMap from "@highcharts/map-collection/custom/world.geo.json";
import MapChart from "../components/MapChart.vue";

mapInit(Highcharts);

const fetchAllChartsAPI = "http://localhost:8081/charts/dashboards";
const refreshChartsAPI = "http://localhost:8081/charts/refresh";
// const dataFetchAPI = "http://localhost:8081/charts/widgets";
const deleteChartAPI = "http://localhost:8081/charts";

import { onMounted } from 'vue';

import Chart from "../components/Chart.vue";
import Navbar from "../components/Navbar.vue";
import "../assets/styles/tailwind.css";
import { ref } from "vue";

const charts = ref(null);

const isVisible = ref(false);


const totals = ref(null);

const loading = ref(false);

async function fetchData() {
  loading.value = true;
  try {
    const [chartsResponse] = await Promise.all([
      axios.get(fetchAllChartsAPI)
    ]);
    console.log(JSON.stringify(chartsResponse))
    totals.value = chartsResponse.data.fetchDataWidget;
    charts.value = chartsResponse.data.charts;
    console.log(totals.value)
  } catch (error) {
    console.error("Error fetching data:", error);
    // Handle errors as needed
  }
  loading.value = false;
  isVisible.value = true;
}

async function refreshData() {
  totals.value = null;
  charts.value = null;
  isVisible.value = false;
  loading.value = true;
  try {
    const [chartsResponse] = await Promise.all([
      axios.get(refreshChartsAPI)
    ]);
    console.log(JSON.stringify(chartsResponse))
    totals.value = chartsResponse.data.fetchDataWidget;
    charts.value = chartsResponse.data.charts;
    console.log(totals.value)
  } catch (error) {
    console.error("Error fetching data:", error);
    // Handle errors as needed
  }
  loading.value = false;
  isVisible.value = true;
}

// async function fetchData() {
//   loading.value = true;
//   try {
//     const [chartsResponse, totalsResponse] = await Promise.all([
//       axios.get(dataFetchAPI),
//       axios.get(fetchAllChartsAPI),
//     ]);
//     totals.value = totalsResponse.data;
//     console.log(totals.value)
//     charts.value = chartsResponse.data;
//   } catch (error) {
//     console.error("Error fetching data:", error);
//     // Handle errors as needed
//   }
//   loading.value = false;
// }

// function deleteChart(index) {
//   const chartId = charts.value[index].id;
//   console.log(chartId);
//   charts.value.splice(index, 1); // Remove the chart from the array
// }

// async function deleteChart(index) {
//   const chartId = charts.value[index].id;
//   axios
//     .post("/api/delete-chart", { id: chartId }) // Adjust the endpoint as necessary
//     .then(() => {
//       charts.value.splice(index, 1); // Remove the chart from the array
//       console.log("Chart deleted successfully");
//     })
//     .catch((error) => console.error("Error deleting chart:", error));
// }

async function deleteChart(index) {
  const chartId = charts.value[index].id;
  try {
    await axios.delete(`${deleteChartAPI}/${chartId}`);
    charts.value.splice(index, 1); // Remove the chart from the array
    console.log("Chart deleted successfully");
  } catch (error) {
    console.error("Error deleting chart:", error);
  }
}

onMounted(fetchData);
</script>
