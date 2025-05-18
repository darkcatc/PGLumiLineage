<template>
  <div class="app-container">
    <el-container>
      <el-header height="60px">
        <app-header />
      </el-header>
      <el-container>
        <el-aside width="220px">
          <app-sidebar />
        </el-aside>
        <el-main>
          <router-view v-slot="{ Component }">
            <transition name="fade" mode="out-in">
              <component :is="Component" />
            </transition>
          </router-view>
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script lang="ts" setup>
import { onMounted } from 'vue';
import AppHeader from '@/components/common/AppHeader.vue';
import AppSidebar from '@/components/common/AppSidebar.vue';

// 组件挂载时执行
onMounted(() => {
  console.log('应用已挂载');
});
</script>

<style lang="scss">
// 全局样式
:root {
  // 主题颜色
  --primary-color: #3498db;
  --secondary-color: #2ecc71;
  --accent-color: #e74c3c;
  --background-color: #f8f9fa;
  --text-color: #2c3e50;
  --border-color: #dcdfe6;
  
  // 暗色主题
  &.dark-theme {
    --primary-color: #2980b9;
    --secondary-color: #27ae60;
    --accent-color: #c0392b;
    --background-color: #1a1a1a;
    --text-color: #ecf0f1;
    --border-color: #34495e;
  }
}

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

html, body {
  height: 100%;
  font-family: 'Helvetica Neue', Helvetica, 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', '微软雅黑', Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  color: var(--text-color);
  background-color: var(--background-color);
}

#app {
  height: 100%;
}

.app-container {
  height: 100%;
  
  .el-container {
    height: 100%;
  }
  
  .el-header {
    background-color: #fff;
    border-bottom: 1px solid var(--border-color);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    padding: 0;
  }
  
  .el-aside {
    background-color: #fff;
    border-right: 1px solid var(--border-color);
    overflow-x: hidden;
  }
  
  .el-main {
    padding: 20px;
    overflow-x: hidden;
  }
}

// 页面过渡动画
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.3s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>
