<template>
  <Navbar></Navbar>

  <div class="flex items-start">
    <!-- Sidebar -->
    <div
      class="flex flex-col w-[360px] h-full items-start gap-12 shrink-0 p-6  border-r border-solid bg-background-accent-2">
      <div class="flex flex-col justify-between h-full w-full">
        <div class="overflow-auto">
          <div class="flex flex-col items-start gap-4 self-stretch">
            <!-- Chart Type Selection -->
            <div class="flex flex-col items-start gap-2 self-stretch">
              <legend class="text-black [font-family:Figtree] text-2xl font-bold leading-[normal]">
                Chart Type
              </legend>
              <div class="flex flex-col gap-2">
                <label class="flex items-center gap-2">
                  <input type="radio" value="bar" v-model="selectedChartType" class="[font-family:Figtree]" />
                  Bar Chart
                </label>
                <label class="flex items-center gap-2">
                  <input type="radio" value="pie" v-model="selectedChartType" class="[font-family:Figtree]" />
                  Pie Chart
                </label>
                <label class="flex items-center gap-2">
                  <input type="radio" value="line" v-model="selectedChartType" class="[font-family:Figtree]" />
                  Line Chart
                </label>
                <label class="flex items-center gap-2">
                  <input type="radio" value="map" v-model="selectedChartType" class="[font-family:Figtree]" />
                  Map Chart
                </label>
              </div>
            </div>

            <!-- KPI Selection -->
            <div class="flex flex-col items-start gap-2 self-stretch">
              <legend class="text-black [font-family:Figtree] text-2xl font-bold leading-[normal]">
                KPI
              </legend>
              <div class="flex flex-col gap-2">
                <label class="flex items-center gap-2">
                  <input type="radio" :value="{
                    fact_name: 'fctSales',
                    fact_column: 'Sales Amount',
                  }" v-model="selectedFact" />
                  Sales Amount
                </label>
                <label class="flex items-center gap-2">
                  <input type="radio" :value="{
                    fact_name: 'fctSales',
                    fact_column: 'Quantity',
                  }" v-model="selectedFact" />
                  Quantity
                </label>
              </div>
            </div>

            <hr class="bg-border" />

            <!-- Time section -->
            <div class="flex flex-col items-start gap-2 self-stretch">
              <legend class="text-black [font-family:Figtree] text-2xl font-bold leading-[normal]">
                Time
              </legend>
              <div class="flex flex-col gap-2">
                <div>
                  <!-- radio -->
                  <label class="flex items-center gap-2">
                    <input type="radio" value="year_number" v-model="dimTime.dim_column" />
                    Year
                  </label>

                  <!-- checkbox -->
                  <div class="pl-5" v-if="dimTime.dim_column == 'year_number'">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('year', $event)" :checked="allSelected('year')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2" v-for="year in years" :key="year">
                      <input type="checkbox" :value="{ year_number: year }" v-model="dimTime.dim_condition" />
                      {{ year }}
                    </label>
                  </div>
                </div>

                <div>
                  <label class="flex items-center gap-2">
                    <input type="radio" value="quarter_of_year" v-model="dimTime.dim_column" />
                    Quarter
                  </label>
                  <div class="pl-5" v-if="dimTime.dim_column == 'quarter_of_year'">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('quarter', $event)" :checked="allSelected('quarter')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2" v-for="quarter in quarters" :key="quarter">
                      <input type="checkbox" :value="{ quarter_of_year: quarter.quarter, year_number: quarter.year }"
                        v-model="dimTime.dim_condition" />
                      Q{{ quarter.quarter }} - {{ quarter.year }}
                    </label>
                  </div>
                </div>
              </div>
            </div>

            <!-- Customer section -->
            <div class="flex flex-col items-start gap-2 self-stretch">
              <legend class="text-black [font-family:Figtree] text-2xl font-bold leading-[normal]">
                Customer
              </legend>
              <div class="flex flex-col gap-2">
                <!-- gender -->
                <div>
                  <!-- radio -->
                  <label class="flex items-center gap-2">
                    <input type="radio" value="gender" v-model="dimCustomer.dim_column" />
                    Gender
                  </label>

                  <!-- checkbox -->
                  <div class="pl-5" v-if="dimCustomer.dim_column == 'gender'">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('gender', $event)" :checked="allSelected('gender')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2">
                      <input type="checkbox" :value="{ gender: 'M' }" v-model="dimCustomer.dim_condition" />
                      Male
                    </label>
                    <label class="flex items-center gap-2">
                      <input type="checkbox" :value="{ gender: 'F' }" v-model="dimCustomer.dim_condition" />
                      Female
                    </label>
                  </div>
                </div>

                <!-- education -->
                <div>
                  <!-- radio -->
                  <label class="flex items-center gap-2">
                    <input type="radio" value="education" v-model="dimCustomer.dim_column" />
                    Education
                  </label>

                  <!-- checkbox -->
                  <div class="pl-5" v-if="dimCustomer.dim_column == 'education'">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('education', $event)"
                        :checked="allSelected('education')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2" v-for="education in education" :key="education">
                      <input type="checkbox" :value="{ education: education }" v-model="dimCustomer.dim_condition" />
                      {{ education }}
                    </label>
                  </div>
                </div>

                <!-- occupation -->
                <div>
                  <!-- radio -->
                  <label class="flex items-center gap-2">
                    <input type="radio" value="occupation" v-model="dimCustomer.dim_column" />
                    Occupation
                  </label>

                  <!-- checkbox -->
                  <div class="pl-5" v-if="dimCustomer.dim_column == 'occupation'">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('occupation', $event)"
                        :checked="allSelected('occupation')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2" v-for="occupation in occupation" :key="occupation">
                      <input type="checkbox" :value="{ occupation: occupation }" v-model="dimCustomer.dim_condition" />
                      {{ occupation }}
                    </label>
                  </div>
                </div>
              </div>
            </div>

            <!-- Product section -->
            <div class="flex flex-col items-start gap-2 self-stretch">
              <legend class="text-black [font-family:Figtree] text-2xl font-bold leading-[normal]">
                Product
              </legend>
              <div class="flex flex-col gap-2">
                <!-- Product category -->
                <div>
                  <!-- radio -->
                  <label class="flex items-center gap-2">
                    <input type="radio" value="product_category_name" v-model="dimProduct.dim_column" />
                    Product Category
                  </label>

                  <!-- checkbox -->
                  <div class="pl-5" v-if="dimProduct.dim_column == 'product_category_name'">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('product_category_name', $event)"
                        :checked="allSelected('product_category_name')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2" v-for="category in categories" :key="category">
                      <input type="checkbox" :value="{ product_category_name: category }"
                        v-model="dimProduct.dim_condition" />
                      {{ category }}
                    </label>
                  </div>
                </div>

                <!-- Product subcategory -->
                <div>
                  <!-- radio -->
                  <label class="flex items-center gap-2">
                    <input type="radio" value="product_subcategory_name" v-model="dimProduct.dim_column" />
                    Product Subcategory
                  </label>

                  <!-- checkbox -->
                  <div class="pl-5" v-if="dimProduct.dim_column == 'product_subcategory_name'">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('product_subcategory_name', $event)"
                        :checked="allSelected('product_subcategory_name')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2" v-for="subcategory in subcategories" :key="subcategory">
                      <input type="checkbox" :value="{ product_subcategory_name: subcategory }"
                        v-model="dimProduct.dim_condition" />
                      {{ subcategory }}
                    </label>
                  </div>
                </div>
              </div>
            </div>

            <!-- Promotion section -->
            <div class="flex flex-col items-start gap-2 self-stretch">
              <legend class="text-black [font-family:Figtree] text-2xl font-bold leading-[normal]">
                Promotion
              </legend>
              <div class="flex flex-col gap-2">
                <!-- Promotion type -->
                <div>
                  <!-- radio -->
                  <label class="flex items-center gap-2">
                    <input type="radio" value="type" v-model="dimPromotion.dim_column" />
                    Promotion type
                  </label>

                  <!-- checkbox -->
                  <div class="pl-5" v-if="dimPromotion.dim_column == 'type'">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('type', $event)" :checked="allSelected('type')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2" v-for="promotion in promotions" :key="promotion">
                      <input type="checkbox" :value="{ type: promotion }" v-model="dimPromotion.dim_condition" />
                      {{ promotion }}
                    </label>
                  </div>
                </div>
              </div>
            </div>

            <!-- Country section -->
            <div class="flex flex-col items-start gap-2 self-stretch">
              <legend class="text-black [font-family:Figtree] text-2xl font-bold leading-[normal]">
                Location
              </legend>
              <div class="flex flex-col gap-2">
                <!-- Promotion type -->
                <div>
                  <!-- radio -->
                  <label class="flex items-center gap-2">
                    <input type="radio" :value="getAddressDimValue" v-model="dimLocation.dim_column" />
                    Country
                  </label>

                  <!-- checkbox -->
                  <div class="pl-5" v-if="dimLocation.dim_column == 'country_name' ||
                    dimLocation.dim_column == 'country_code'
                    ">
                    <label class="flex items-center gap-2">
                      <input type="checkbox" @change="selectAll('country', $event)" :checked="allSelected('country')" />
                      Select All
                    </label>
                    <label class="flex items-center gap-2" v-for="country in countries" :key="country">
                      <input type="checkbox" :value="{
                        country_name: country.country_name,
                        country_code: country.country_code,
                      }" v-model="dimLocation.dim_condition" />
                      {{ country.country_name }}
                    </label>
                  </div>
                </div>
              </div>
            </div>

            <button @click="clearSelections" class="text-primary [font-family:Figtree] text-base font-bold leading-5">
              Clear Selection
            </button>
          </div>
        </div>
        <div class="fixed-button-container px-6 py-6 bg-background-accent-2 border-r border-solid">
          <button @click="fetchData"
            class="w-[311px] flex justify-center w-full items-center gap-2.5 self-stretch bg-primary px-6 py-3 rounded-xl text-white [font-family:Figtree] text-base font-bold leading-6">
            Visualize
          </button>
        </div>
      </div>
    </div>

    <!-- Chart Name field & Chart Visualization -->
    <div class="flex justify-start items-center h-full flex-col items-start gap-6 flex-[1_0_0] p-6">
      <!-- Chart name -->
      <div class="flex items-start gap-6 self-stretch">
        <input
          class="flex items-center gap-2.5 flex-[1_0_0] border border-border bg-white [font-family:Figtree] px-6 py-3 rounded-lg border-solid"
          type="text" v-model="chartName" placeholder="Enter chart name" />
        <button @click="addChart" :disabled="!canAddChart" :class="canAddChart ? 'button-normal' : 'button-disabled'"
          class="flex justify-center items-center gap-2.5 self-stretch bg-primary px-6 py-3 rounded-xl text-white [font-family:Figtree] text-base font-bold leading-6">
          Add to dashboard
        </button>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="spinner-container">
        <div class="spinner"></div>
      </div>

      <!-- Chart -->
      <Bar v-if="chartData && selectedChartType === 'bar'" :data="chartData" />
      <Line v-if="chartData && selectedChartType === 'line'" :data="chartData" />
      <div v-if="chartData && selectedChartType === 'pie'" class="w-[600px]">
        <Pie :data="chartData" />
      </div>
      <div v-if="chartData && selectedChartType === 'map'" class="w-[1000px] h-[600px]">
        <MapChart :data="chartData" :chartId="0" />
      </div>
    </div>
  </div>
</template>

<script setup>
import axios from 'axios';
import { useRouter } from "vue-router";
const router = useRouter();

import MapChart from "@/components/MapChart.vue";
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

import Navbar from "../components/Navbar.vue";
import "../assets/styles/tailwind.css";

import { ref, computed, watch } from "vue";


// APT
const previewChartAPI = "http://localhost:8081/charts/preview";
const saveChartAPI = "http://localhost:8081/charts/save-chart";


// const selectedCustomerDim = ref("");
// const selectedPromotion = ref("");
// const selectedTimeDim = ref("");
// const selectedProductDim = ref("");
// const selectedAddressDim = ref("");
const chartName = ref("");
const selectedFact = ref("");
const chartData = ref(null);
const chartState = ref(null);
const selectedChartType = ref("bar"); // default to bar chart

const loading = ref(false);


// List of filter value
const quarters = ref([]);
const years = ref([]);
const startYear = 2011;
const endYear = 2014;
for (let year = startYear; year <= endYear; year++) {
  years.value.push(year);
  for (let quarter = 1; quarter <= 4; quarter++) {
    quarters.value.push({ quarter, year });
  }
}

const occupation = ref([
  "Management",
  "Professional",
  "Clerical",
  "Manual",
  "Skilled Manual",
]);
const education = ref([
  "High School",
  "Partial High School",
  "Graduate Degree",
  "Bachelors",
  "Partial College",
]);
const categories = ref(["Bikes", "Clothing", "Accessories", "Components"]);
const subcategories = ref([
  "Road Bikes",
  "Road Frames",
  "Helmets",
  "Jerseys",
  "Caps",
  "Mountain Frames",
  "Socks",
  "Headsets",
  "Wheels",
  "Bottom Brackets",
  "Touring Frames",
  "Mountain Bikes",
  "Pedals",
  "Derailleurs",
  "Chains",
  "Pumps",
  "Hydration Packs",
  "Bottles and Cages",
  "Forks",
  "Vests",
  "Brakes",
  "Saddles",
  "Tights",
  "Gloves",
  "Panniers",
  "Touring Bikes",
  "Bike Racks",
  "Fenders",
  "Lights",
  "Cranksets",
  "Shorts",
  "Locks",
  "Bike Stands",
  "Bib-Shorts",
  "Handlebars",
  "Tires and Tubes",
  "Cleaners",
]);
const promotions = ref([
  "Excess Inventory",
  "Discontinued Product",
  "No Discount",
  "New Product",
  "Volume Discount",
  "Seasonal Discount",
]);
const countries = ref([
  { country_name: "United States", country_code: "USA" },
  { country_name: "Germany", country_code: "DEU" },
  { country_name: "France", country_code: "FRA" },
  { country_name: "Canada", country_code: "CAN" },
  { country_name: "Australia", country_code: "AUS" },
  { country_name: "United Kingdom", country_code: "GBR" },
]);

// List of DIM
const dimTime = ref({
  dim_name: "dimdate",
  dim_column: "",
  dim_key: "date_key",
  ref_fact_key: "order_date_key",
  dim_condition: [],
});

const dimCustomer = ref({
  dim_name: "dimcustomer",
  dim_column: "",
  dim_key: "customer_key",
  ref_fact_key: "customer_key",
  dim_condition: [],
});

const dimPromotion = ref({
  dim_name: "dimpromotion",
  dim_column: "",
  dim_key: "promotion_key",
  ref_fact_key: "promotion_key",
  dim_condition: [],
});

const dimProduct = ref({
  dim_name: "dimproduct",
  dim_column: "",
  dim_key: "product_key",
  ref_fact_key: "product_key",
  dim_condition: [],
});

const dimLocation = ref({
  dim_name: "dimaddress",
  dim_column: "",
  dim_key: "address_key",
  ref_fact_key: "ship_address_key",
  dim_condition: [],
});

// Select All
const selectAll = (section, event) => {
  const isChecked = event.target.checked;
  if (section === "year") {
    dimTime.value.dim_condition = isChecked
      ? years.value.map((year) => ({ year_number: year }))
      : [];
  } else if (section === "quarter") {
    dimTime.value.dim_condition = isChecked
      ? quarters.value.map((q) => ({ quarter_of_year: q.quarter, year_number: q.year }))
      : [];
  } else if (section === "gender") {
    dimCustomer.value.dim_condition = isChecked
      ? ["M", "F"].map((gender) => ({ gender }))
      : [];
  } else if (section === "education") {
    dimCustomer.value.dim_condition = isChecked
      ? education.value.map((edu) => ({ education: edu }))
      : [];
  } else if (section === "occupation") {
    dimCustomer.value.dim_condition = isChecked
      ? occupation.value.map((occ) => ({ occupation: occ }))
      : [];
  } else if (section === "product_category_name") {
    dimProduct.value.dim_condition = isChecked
      ? categories.value.map((cat) => ({ product_category_name: cat }))
      : [];
  } else if (section === "product_subcategory_name") {
    dimProduct.value.dim_condition = isChecked
      ? subcategories.value.map((sub) => ({ product_subcategory_name: sub }))
      : [];
  } else if (section === "type") {
    dimPromotion.value.dim_condition = isChecked
      ? promotions.value.map((promo) => ({ type: promo }))
      : [];
  } else if (section === "country") {
    dimLocation.value.dim_condition = isChecked
      ? countries.value.map((country) => ({
        country_name: country.country_name,
        country_code: country.country_code,
      }))
      : [];
  }
};

const allSelected = (section) => {
  if (section === "year") {
    return years.value.length === dimTime.value.dim_condition.length;
  } else if (section === "quarter") {
    return quarters.value.length === dimTime.value.dim_condition.length;
  } else if (section === "gender") {
    return 2 === dimCustomer.value.dim_condition.length;
  } else if (section === "education") {
    return education.value.length === dimCustomer.value.dim_condition.length;
  } else if (section === "occupation") {
    return occupation.value.length === dimCustomer.value.dim_condition.length;
  } else if (section === "product_category_name") {
    return categories.value.length === dimProduct.value.dim_condition.length;
  } else if (section === "product_subcategory_name") {
    return subcategories.value.length === dimProduct.value.dim_condition.length;
  } else if (section === "type") {
    return promotions.value.length === dimPromotion.value.dim_condition.length;
  } else if (section === "country") {
    return countries.value.length === dimLocation.value.dim_condition.length;
  }
  return false;
};

watch(selectedChartType, (newType) => {
  // Reset dimensions if chart type changes
  clearSelections();
});

const selectedDimensions = computed(() => {
  return [
    dimTime.value,
    dimLocation.value,
    dimProduct.value,
    dimPromotion.value,
    dimCustomer.value,
  ].filter((dim) => dim.dim_column !== "");
});

// Watchers to clear dim_condition when dim_column changes
function resetConditionOnColumnChange(dimObject) {
  watch(
    () => dimObject.value.dim_column,
    () => {
      dimObject.value.dim_condition = [];
    }
  );
}
resetConditionOnColumnChange(dimTime);
resetConditionOnColumnChange(dimCustomer);
resetConditionOnColumnChange(dimProduct);
resetConditionOnColumnChange(dimPromotion);
resetConditionOnColumnChange(dimLocation);

const canAddChart = computed(() => {
  // Check conditions to enable the "Add" button
  return (
    chartData.value &&
    selectedFact.value &&
    selectedDimensions.value.length > 0 &&
    chartName.value
  );
});

async function fetchData() {
  chartData.value = null;
  if (
    selectedDimensions.value.length === 0 ||
    selectedDimensions.value.length > 2
  ) {
    alert("Please select one or two dimensions from different tables.");
    return;
  }

  console.log({
    client_chart_metadata: {
      chart_type: selectedChartType.value,
      fact_name: selectedFact.value.fact_name,
      fact_column: selectedFact.value.fact_column,
      dimensions: selectedDimensions.value,
    }
  })

  loading.value = true;
  window.scrollTo(0, 0);
  try {
    const response = await axios.post(previewChartAPI, {
      client_chart_metadata: {
        chart_type: selectedChartType.value,
        fact_name: selectedFact.value.fact_name,
        fact_column: selectedFact.value.fact_column,
        dimensions: selectedDimensions.value,
      }
    });
    chartData.value = response.data.chart;
    console.log(chartData.value)
    chartState.value = response.data.chart_state;
    console.log(chartData)
    console.log(chartState)
  } catch (error) {
    console.error("Error fetching data:", error);
    chartData.value = null; // Reset chart data on error
  }
  loading.value = false;
}


async function addChart() {
  try {
    console.log(1)
    console.log(chartState.value)
    const response = await axios.post(saveChartAPI, {
      chart_state: chartState.value,
      chart_name: chartName.value,
    });
    console.log("Chart added successfully:", response.data);
    // Optionally reset fields after adding
    clearSelections();
    router.push("/dashboard");
  } catch (error) {
    console.error("Error adding chart:", error);
  }
}

function clearSelections() {
  dimTime.value = { ...dimTime.value, dim_column: "", dim_condition: [] };
  dimCustomer.value = {
    ...dimCustomer.value,
    dim_column: "",
    dim_condition: [],
  };
  dimProduct.value = { ...dimProduct.value, dim_column: "", dim_condition: [] };
  dimPromotion.value = {
    ...dimPromotion.value,
    dim_column: "",
    dim_condition: [],
  };
  dimLocation.value = {
    ...dimLocation.value,
    dim_column: "",
    dim_condition: [],
  };
  chartData.value = null;
}

watch(selectedDimensions, (newVal) => {
  console.log(selectedDimensions);
  let maxDims;
  if (selectedChartType.value == "bar" || selectedChartType.value == "line")
    maxDims = 2;
  else maxDims = 1;
  if (selectedDimensions.value.length > maxDims) {
    alert(
      `Please select up to ${maxDims} dimensions for a ${selectedChartType.value} chart.`
    );
    clearSelections();
    return;
  }
});

const chartOptions = {
  responsive: false,
  maintainAspectRatio: true,
};

const getAddressDimValue = computed(() => {
  return selectedChartType.value === "map" ? "country_code" : "country_name";
});
</script>

<style>
.button-disabled {
  background-color: #a1c6ff;
  /* Greyed out */
  cursor: not-allowed;
}

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

.overflow-auto {
  max-height: calc(100vh - 206px);
  overflow-y: auto;
}

.fixed-button-container {
  position: fixed;
  /* Fixed positioning to make it stick */
  bottom: 0px;
  /* Distance from the bottom of the viewport */
  left: 0px;
  /* Distance from the right of the viewport */
  z-index: 1000;
  /* Ensures it stays on top of other content */
}

.height-calculate {
  height: calc(100vh-400px);
}
</style>
