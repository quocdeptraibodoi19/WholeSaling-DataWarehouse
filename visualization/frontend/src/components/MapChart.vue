<template>
  <div :id="'map-container-' + chartId" class="w-full h-full"></div>
</template>

<script setup>
import { onMounted } from "vue";
import Highcharts from "highcharts";
import mapInit from "highcharts/modules/map";
import exportingInit from "highcharts/modules/exporting";
import dataInit from "highcharts/modules/data";
import accessibilityInit from "highcharts/modules/accessibility";

mapInit(Highcharts);
exportingInit(Highcharts);
dataInit(Highcharts);
accessibilityInit(Highcharts);

const props = defineProps({
  data: Array,
  chartId: String,
});

const containerId = `map-container-${props.chartId}`;

// Assuming `jsonData` is the variable that holds your JSON array
// const jsonData = [{ "Country Code": "ARG", 2021: 75.39 }];

const jsonData = props.data.datasets;

onMounted(async () => {
  try {
    const response = await fetch(
      "https://code.highcharts.com/mapdata/custom/world.topo.json"
    );
    if (!response.ok) throw new Error("Network response was not ok");
    const topology = await response.json();
    Highcharts.mapChart(containerId, {
      chart: {
        map: topology,
      },
      title: {
        text: "",
        align: "left",
      },
      mapNavigation: {
        enabled: true,
        buttonOptions: {
          verticalAlign: "bottom",
        },
      },
      colorAxis: {
        type: "linear",
        minColor: "#D4E5FF",
        maxColor: "#052F6E",
        stops: [
          [0, "#D4E5FF"],
          [0.67, "#4444FF"],
          [1, "#052F6E"],
        ],
      },
      series: [
        {
          name: "",
          data: jsonData.map((data) => ({
            code: data["label"],
            value: data["data"] !== null ? parseFloat(data["data"]) : null,
          })),
          joinBy: ["iso-a3", "code"],
          dataLabels: {
            enabled: true,
            format: "{point.value:.0f}",
            filter: {
              operator: ">",
              property: "labelrank",
              value: 250,
            },
            style: {
              fontWeight: "normal",
            },
          },
        },
      ],
    });
  } catch (error) {
    console.error("Failed to load the map data:", error);
  }
});
</script>
