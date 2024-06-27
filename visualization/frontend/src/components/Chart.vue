<template>
  <div class="flex flex-col items-start gap-3 border border-border bg-white p-4 rounded-2xl border-solid h-[400px]">
    <div class="flex justify-between items-center self-stretch">
      <div class="text-text [font-family:Figtree] text-xl font-semibold leading-8">
        {{ chartName }}
        <!-- Dynamically display the chart name -->
      </div>
      <button @click="emitDelete" class="text-text hover:text-red-700">
        X
      </button>
    </div>
    <div class="flex justify-center items-center self-stretch" style="width: 100%; height: 100%">
      <!-- The chart will take 100% of the container's width and height -->
      <Bar v-if="chartType === 'bar'" :data="chartData" />
      <Pie v-if="chartType === 'pie'" :data="chartData" :options="pieChartOptions" />
      <Line v-if="chartType === 'line'" :data="chartData" />
      <MapChart v-if="chartType === 'map'" :data="chartData" :chartId="index" />
      <MapChartRegion v-if="chartType === 'map_region'" :data="chartData" :chartId="index" />
    </div>
  </div>
</template>

<script setup>
import { ref } from "vue";
import MapChart from "./MapChart.vue";
import MapChartRegion from "./MapChartRegion.vue";
import { Bar, Pie, Line } from "vue-chartjs";
import {
  Chart as ChartJS,
  Title,
  Tooltip,
  Legend,
  BarElement,
  PieController,
  ArcElement,
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement,
} from "chart.js";

ChartJS.register(
  Title,
  Tooltip,
  Legend,
  PieController,
  ArcElement,
  BarElement,
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement
);

const chartOptions = {
  responsive: true,
  maintainAspectRatio: true,
};

const pieChartOptions = {
  responsive: false,
  maintainAspectRatio: true,
};

const props = defineProps({
  chartType: String,
  chartData: Object,
  chartName: String,
  index: Number, // Receive the index of the chart
});

const emits = defineEmits(["deleteChart"]);

function emitDelete() {
  emits("deleteChart", props.index); // Emit the index on click
}
</script>
