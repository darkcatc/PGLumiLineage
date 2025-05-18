<template>
  <div class="not-found-page">
    <div class="not-found-container">
      <el-result
        icon="error"
        title="404"
        sub-title="抱歉，您访问的页面不存在"
      >
        <template #extra>
          <el-button type="primary" @click="goHome">返回首页</el-button>
        </template>
      </el-result>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { useRouter } from 'vue-router';

const router = useRouter();

// 返回首页
const goHome = () => {
  router.push('/');
};
</script>

<style lang="scss" scoped>
.not-found-page {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  
  .not-found-container {
    max-width: 600px;
    width: 100%;
    padding: 20px;
    text-align: center;
  }
}
</style>
