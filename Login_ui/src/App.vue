<template>
  <div id="app">
    <router-view />
  </div>
</template>

<script>
export default {
  name: 'App'
}
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body {
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  /* background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); */
  background-color: #f0f2f5;
  min-height: 100vh;
}

#app {
  min-height: 100vh;
  /* display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px; */
}
</style>