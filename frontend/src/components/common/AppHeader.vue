<template>
  <div class="app-header">
    <div class="logo">
      <router-link to="/">
        <img src="@/assets/logo.png" alt="PGLumiLineage" />
        <span>PGLumiLineage</span>
      </router-link>
    </div>
    <div class="search-bar">
      <el-input
        v-model="searchQuery"
        placeholder="搜索对象（表、列、视图等）"
        prefix-icon="el-icon-search"
        clearable
        @keyup.enter="handleSearch"
      >
        <template #prefix>
          <el-icon><Search /></el-icon>
        </template>
      </el-input>
    </div>
    <div class="header-actions">
      <el-tooltip content="切换主题" placement="bottom">
        <el-button circle @click="toggleTheme">
          <el-icon><Moon v-if="isDarkTheme" /><Sunny v-else /></el-icon>
        </el-button>
      </el-tooltip>
      <el-tooltip content="帮助" placement="bottom">
        <el-button circle @click="showHelp">
          <el-icon><QuestionFilled /></el-icon>
        </el-button>
      </el-tooltip>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed } from 'vue';
import { useRouter } from 'vue-router';
import { Search, Moon, Sunny, QuestionFilled } from '@element-plus/icons-vue';

// 路由实例
const router = useRouter();

// 搜索查询
const searchQuery = ref('');

// 当前主题
const isDarkTheme = ref(localStorage.getItem('theme') === 'dark');

// 切换主题
const toggleTheme = () => {
  isDarkTheme.value = !isDarkTheme.value;
  if (isDarkTheme.value) {
    document.documentElement.classList.add('dark-theme');
    localStorage.setItem('theme', 'dark');
  } else {
    document.documentElement.classList.remove('dark-theme');
    localStorage.setItem('theme', 'light');
  }
};

// 处理搜索
const handleSearch = () => {
  if (!searchQuery.value.trim()) return;
  
  // 简单的搜索逻辑，实际应用中可能需要更复杂的处理
  if (searchQuery.value.includes('.')) {
    // 假设格式为 database.schema.table.column 或 database.schema.table
    const parts = searchQuery.value.split('.');
    if (parts.length >= 3) {
      // 假设是表或视图
      router.push({
        name: 'object-details',
        params: {
          type: 'table',
          fqn: encodeURIComponent(searchQuery.value)
        }
      });
    } else if (parts.length === 4) {
      // 假设是列
      router.push({
        name: 'object-details',
        params: {
          type: 'column',
          fqn: encodeURIComponent(searchQuery.value)
        }
      });
    }
  } else {
    // 简单搜索，跳转到血缘图页面并传递搜索参数
    router.push({
      name: 'lineage',
      query: { search: searchQuery.value }
    });
  }
  
  // 清空搜索框
  searchQuery.value = '';
};

// 显示帮助
const showHelp = () => {
  // 实现帮助对话框
  ElMessageBox.alert(
    '数据血缘分析平台使用指南：<br><br>' +
    '1. 在搜索框中输入对象名称（表、列、视图等）进行搜索<br>' +
    '2. 点击节点可查看详细信息<br>' +
    '3. 使用路径查找功能可查找两个对象之间的数据流路径<br>' +
    '4. 使用鼠标滚轮或触控板手势可缩放图形<br>' +
    '5. 按住鼠标左键拖拽可平移图形<br>',
    '使用帮助',
    {
      dangerouslyUseHTMLString: true,
      confirmButtonText: '我知道了'
    }
  );
};

// 初始化主题
if (isDarkTheme.value) {
  document.documentElement.classList.add('dark-theme');
} else {
  document.documentElement.classList.remove('dark-theme');
}
</script>

<style lang="scss" scoped>
.app-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 100%;
  padding: 0 20px;
  
  .logo {
    a {
      display: flex;
      align-items: center;
      text-decoration: none;
      color: var(--text-color);
      font-weight: bold;
      font-size: 18px;
      
      img {
        height: 32px;
        margin-right: 10px;
      }
    }
  }
  
  .search-bar {
    flex: 1;
    max-width: 500px;
    margin: 0 20px;
  }
  
  .header-actions {
    display: flex;
    gap: 10px;
  }
}
</style>
