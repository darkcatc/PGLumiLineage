<template>
  <div class="app-sidebar">
    <el-menu
      :default-active="activeMenu"
      class="sidebar-menu"
      :router="true"
      :collapse="isCollapse"
    >
      <el-menu-item index="/">
        <el-icon><HomeFilled /></el-icon>
        <template #title>首页</template>
      </el-menu-item>
      
      <el-menu-item index="/lineage">
        <el-icon><Share /></el-icon>
        <template #title>数据血缘图</template>
      </el-menu-item>
      
      <el-menu-item index="/path-finder">
        <el-icon><Connection /></el-icon>
        <template #title>路径查找</template>
      </el-menu-item>
      
      <el-sub-menu index="objects">
        <template #title>
          <el-icon><Grid /></el-icon>
          <span>对象浏览器</span>
        </template>
        
        <el-menu-item index="/objects/databases">
          <el-icon><Coin /></el-icon>
          <span>数据库</span>
        </el-menu-item>
        
        <el-menu-item index="/objects/schemas">
          <el-icon><Folder /></el-icon>
          <span>模式</span>
        </el-menu-item>
        
        <el-menu-item index="/objects/tables">
          <el-icon><List /></el-icon>
          <span>表</span>
        </el-menu-item>
        
        <el-menu-item index="/objects/views">
          <el-icon><View /></el-icon>
          <span>视图</span>
        </el-menu-item>
      </el-sub-menu>
      
      <el-sub-menu index="analysis">
        <template #title>
          <el-icon><DataAnalysis /></el-icon>
          <span>分析工具</span>
        </template>
        
        <el-menu-item index="/analysis/impact">
          <el-icon><TrendCharts /></el-icon>
          <span>影响分析</span>
        </el-menu-item>
        
        <el-menu-item index="/analysis/dependencies">
          <el-icon><Link /></el-icon>
          <span>依赖分析</span>
        </el-menu-item>
      </el-sub-menu>
    </el-menu>
    
    <div class="sidebar-footer">
      <el-tooltip :content="isCollapse ? '展开菜单' : '收起菜单'" placement="right">
        <el-button circle @click="toggleCollapse">
          <el-icon>
            <Fold v-if="isCollapse" />
            <Expand v-else />
          </el-icon>
        </el-button>
      </el-tooltip>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed } from 'vue';
import { useRoute } from 'vue-router';
import {
  HomeFilled,
  Share,
  Connection,
  Grid,
  Coin,
  Folder,
  List,
  View,
  DataAnalysis,
  TrendCharts,
  Link,
  Fold,
  Expand
} from '@element-plus/icons-vue';

// 路由实例
const route = useRoute();

// 菜单折叠状态
const isCollapse = ref(localStorage.getItem('sidebarCollapsed') === 'true');

// 当前激活的菜单项
const activeMenu = computed(() => {
  return route.path;
});

// 切换菜单折叠状态
const toggleCollapse = () => {
  isCollapse.value = !isCollapse.value;
  localStorage.setItem('sidebarCollapsed', isCollapse.value.toString());
};
</script>

<style lang="scss" scoped>
.app-sidebar {
  height: 100%;
  display: flex;
  flex-direction: column;
  
  .sidebar-menu {
    flex: 1;
    border-right: none;
  }
  
  .sidebar-footer {
    padding: 10px;
    display: flex;
    justify-content: center;
    border-top: 1px solid var(--border-color);
  }
}
</style>
