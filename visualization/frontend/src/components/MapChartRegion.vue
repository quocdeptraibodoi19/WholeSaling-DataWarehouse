<template>
  <div :id="'map-container-' + chartId" class="w-full h-full"></div>
</template>

<script setup>
import { onMounted } from "vue";
import Highcharts from "highcharts/highmaps"; // Import Highmaps instead of Highcharts
import mapInit from "highcharts/modules/map";
import exportingInit from "highcharts/modules/exporting";
import dataInit from "highcharts/modules/data";
import accessibilityInit from "highcharts/modules/accessibility";

// Initialize the necessary modules for Highmaps
mapInit(Highcharts);
exportingInit(Highcharts);
dataInit(Highcharts);
accessibilityInit(Highcharts);

const props = defineProps({
  data: Array,
  chartId: String,
});

const containerId = `map-container-${props.chartId}`;

const jsonData = props.data.datasets;
console.log(jsonData);

onMounted(async () => {
  try {
    const mapUrl = "https://code.highcharts.com/mapdata/custom/world-continents.topo.json"; // Adjust the URL to a map of continents
    const response = await fetch(mapUrl);
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
        min: 0,
        minColor: "#D4E5FF",
        maxColor: "#052F6E",
        stops: [
          [0, "#D4E5FF"],
          [0.67, "#4444FF"],
          [1, "#052F6E"],
        ],
      },
      series: [{
        data: jsonData.map(data => ({
          code: data['label'], // Make sure 'label' matches the continent codes in your map data
          value: data['data']
        })),
        joinBy: ['hc-key', 'code'],
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
        tooltip: {
          valueSuffix: ''
        }
      }]
    });
  } catch (error) {
    console.error("Failed to load the map data:", error);
  }
});
</script>
