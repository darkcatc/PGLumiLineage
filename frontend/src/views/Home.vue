<template>
  <div class="home-page">
    <div class="welcome-section">
      <h1>PGLumiLineage</h1>
      <h2>PostgreSQL 自动化数据血缘分析与知识图谱构建平台</h2>
      <p>通过大语言模型和图数据库技术，从 PostgreSQL 日志和元数据中自动提取、分析并可视化数据血缘关系</p>
      
      <div class="action-buttons">
        <el-button type="primary" size="large" @click="goToLineage">
          <el-icon><Share /></el-icon>
          浏览数据血缘图
        </el-button>
        <el-button size="large" @click="goToPathFinder">
          <el-icon><Connection /></el-icon>
          查找数据路径
        </el-button>
      </div>
    </div>
    
    <div class="features-section">
      <h3>核心功能</h3>
      <div class="feature-cards">
        <el-card class="feature-card">
          <template #header>
            <div class="card-header">
              <el-icon><Document /></el-icon>
              <span>自动化日志处理</span>
            </div>
          </template>
          <div class="card-content">
            自动收集和处理 PostgreSQL 运行时日志，提取 SQL 语句并进行范式化处理
          </div>
        </el-card>
        
        <el-card class="feature-card">
          <template #header>
            <div class="card-header">
              <el-icon><DataAnalysis /></el-icon>
              <span>LLM 驱动的关系提取</span>
            </div>
          </template>
          <div class="card-content">
            利用大语言模型（阿里云通义千问）分析 SQL 语句，提取字段级别的数据血缘关系
          </div>
        </el-card>
        
        <el-card class="feature-card">
          <template #header>
            <div class="card-header">
              <el-icon><Share /></el-icon>
              <span>知识图谱构建</span>
            </div>
          </template>
          <div class="card-content">
            在 Apache AGE 图数据库中构建数据血缘知识图谱，支持复杂的图查询和分析
          </div>
        </el-card>
        
        <el-card class="feature-card">
          <template #header>
            <div class="card-header">
              <el-icon><View /></el-icon>
              <span>交互式可视化</span>
            </div>
          </template>
          <div class="card-content">
            基于 AntV G6 的交互式数据血缘图可视化，支持多种布局和交互方式
          </div>
        </el-card>
      </div>
    </div>
    
    <div class="stats-section">
      <h3>系统统计</h3>
      <div class="stat-cards">
        <el-card class="stat-card">
          <h4>{{ stats.databaseCount }}</h4>
          <p>数据库</p>
        </el-card>
        
        <el-card class="stat-card">
          <h4>{{ stats.tableCount }}</h4>
          <p>表和视图</p>
        </el-card>
        
        <el-card class="stat-card">
          <h4>{{ stats.columnCount }}</h4>
          <p>列</p>
        </el-card>
        
        <el-card class="stat-card">
          <h4>{{ stats.relationCount }}</h4>
          <p>血缘关系</p>
        </el-card>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted } from 'vue';
import { useRouter } from 'vue-router';
import { Share, Connection, Document, DataAnalysis, View } from '@element-plus/icons-vue';

// 路由实例
const router = useRouter();

// 系统统计数据
const stats = ref({
  databaseCount: '0',
  tableCount: '0',
  columnCount: '0',
  relationCount: '0'
});

// 导航到血缘图页面
const goToLineage = () => {
  router.push('/lineage');
};

// 导航到路径查找页面
const goToPathFinder = () => {
  router.push('/path-finder');
};

// 获取系统统计数据
const fetchStats = async () => {
  try {
    // 这里应该调用API获取实际的统计数据
    // 暂时使用模拟数据
    setTimeout(() => {
      stats.value = {
        databaseCount: '5+',
        tableCount: '1,000+',
        columnCount: '10,000+',
        relationCount: '50,000+'
      };
    }, 500);
  } catch (error) {
    console.error('获取统计数据失败:', error);
  }
};

// 组件挂载时获取统计数据
onMounted(() => {
  fetchStats();
});
</script>

<style lang="scss" scoped>
.home-page {
  padding: 20px 0;
  
  .welcome-section {
    text-align: center;
    padding: 40px 20px;
    background-color: #f0f8ff;
    border-radius: 8px;
    margin-bottom: 40px;
    
    h1 {
      font-size: 36px;
      font-weight: 700;
      margin: 0 0 10px;
      color: var(--primary-color);
    }
    
    h2 {
      font-size: 20px;
      font-weight: 600;
      margin: 0 0 20px;
      color: var(--text-color);
    }
    
    p {
      font-size: 16px;
      max-width: 800px;
      margin: 0 auto 30px;
      color: #666;
    }
    
    .action-buttons {
      display: flex;
      justify-content: center;
      gap: 20px;
    }
  }
  
  .features-section,
  .stats-section {
    margin-bottom: 40px;
    
    h3 {
      font-size: 24px;
      font-weight: 600;
      margin: 0 0 20px;
      color: var(--text-color);
      text-align: center;
    }
  }
  
  .feature-cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 20px;
    
    .feature-card {
      height: 100%;
      
      .card-header {
        display: flex;
        align-items: center;
        
        .el-icon {
          margin-right: 8px;
          font-size: 18px;
          color: var(--primary-color);
        }
        
        span {
          font-size: 16px;
          font-weight: 600;
        }
      }
      
      .card-content {
        color: #666;
        line-height: 1.5;
      }
    }
  }
  
  .stat-cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 20px;
    
    .stat-card {
      text-align: center;
      
      h4 {
        font-size: 32px;
        font-weight: 700;
        margin: 0 0 10px;
        color: var(--primary-color);
      }
      
      p {
        font-size: 16px;
        margin: 0;
        color: #666;
      }
    }
  }
}

@media (max-width: 768px) {
  .home-page {
    .welcome-section {
      padding: 30px 15px;
      
      h1 {
        font-size: 28px;
      }
      
      h2 {
        font-size: 18px;
      }
      
      .action-buttons {
        flex-direction: column;
        align-items: center;
      }
    }
    
    .feature-cards,
    .stat-cards {
      grid-template-columns: 1fr;
    }
  }
}
</style>
