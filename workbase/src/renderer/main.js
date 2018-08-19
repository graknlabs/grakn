import Vue from 'vue';
import VueRouter from 'vue-router';

import store from './store';

// UI Elements
import LoadingButton from './components/UIElements/LoadingButton.vue';


// Modules
import { routes } from './routes';
import CustomPlugins from './customPlugins/';

Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

const ENGINE_AUTHENTICATED = false;
const LANDING_PAGE = '/develop/data';

// Disable devtools message
Vue.config.devtools = false;

// Register plugins
Vue.use(VueRouter);

// Add notification properties to Vue instance
CustomPlugins.registerNotifications();

// Register UIElements globally
Vue.component('loading-button', LoadingButton);

// Define a Vue Router and map all the routes to components - as defined in the routes.js file.
const router = new VueRouter({
  linkActiveClass: 'active',
  routes,
});

// Set state variables in global store - this needs to happen before everything else
store.commit('setAuthentication', ENGINE_AUTHENTICATED);
store.commit('setLandingPage', LANDING_PAGE);
store.commit('loadLocalCredentials', ENGINE_AUTHENTICATED);

// Before loading a new route check if the user is authorised
router.beforeEach((to, from, next) => {
  if (to.path === '/login') next();
  if (store.getters.isAuthorised) next();
  else next('/login');
});

function initialiseStore() {
  this.$store.dispatch('initGrakn');
  this.$router.push(LANDING_PAGE);
}

new Vue({
  router,
  store,
  created: initialiseStore,
}).$mount('#grakn-app');
