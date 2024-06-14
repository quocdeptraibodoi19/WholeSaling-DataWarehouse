import { createStore } from 'vuex';
import axios from 'axios';

const fetchAllChartsAPI = "http://localhost:8081/charts/";
const dataFetchAPI = "http://localhost:8081/charts/data-fetch";

export default createStore({
    state: {
        charts: null,
        totals: null,
        loading: false
    },
    mutations: {
        setCharts(state, charts) {
            state.charts = charts;
        },
        setTotals(state, totals) {
            state.totals = totals;
        },
        setLoading(state, loading) {
            state.loading = loading;
        }
    },
    actions: {
        // async fetchCharts({ commit }) {
        //     commit('setLoading', true);
        //     try {
        //         const response = await axios.get(fetchAllChartsAPI);
        //         commit('setCharts', response.data);
        //     } catch (error) {
        //         console.error("Error fetching charts data:", error);
        //     }
        //     finally {
        //         console.log(1000)
        //         commit('setLoading', false); // Set loading false in the finally block
        //     }
        // },
        // async fetchTotals({ commit }) {
        //     commit('setLoading', true);
        //     try {
        //         const response = await axios.get(dataFetchAPI);
        //         commit('setTotals', response.data);
        //     } catch (error) {
        //         console.error("Error fetching totals data:", error);
        //     }
        //     finally {
        //         commit('setLoading', false);
        //     }
        // }
        async fetchData({ commit }) {
            commit('setLoading', true);
            try {
                const [chartsResponse, totalsResponse] = await Promise.all([
                    axios.get(fetchAllChartsAPI),
                    axios.get(dataFetchAPI)
                ]);
                commit('setCharts', chartsResponse.data);
                commit('setTotals', totalsResponse.data);
            } catch (error) {
                console.error("Combined fetch error:", error);
            } finally {
                commit('setLoading', false);
            }
        }
    }
});
