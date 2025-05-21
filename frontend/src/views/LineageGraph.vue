<template>
  <div class="lineage-graph-page">
    <div class="page-header">
      <h2>数据血缘图</h2>
      <div class="search-filters">
        <el-form :inline="true" :model="queryParams" class="filter-form">
          <el-form-item label="根节点类型">
            <el-select v-model="queryParams.root_node_type" placeholder="选择节点类型">
              <el-option v-for="item in nodeTypeOptions" :key="item.value" :label="item.label" :value="item.value" />
            </el-select>
          </el-form-item>
          <el-form-item label="全限定名">
            <el-input v-model="queryParams.root_node_fqn" placeholder="输入对象全限定名" clearable />
          </el-form-item>
          <el-form-item label="查询深度">
            <el-slider v-model="queryParams.depth" :min="1" :max="5" :step="1" show-stops />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="fetchLineageGraph">查询</el-button>
            <el-button @click="resetQuery">重置</el-button>
          </el-form-item>
        </el-form>
      </div>
    </div>
    
    <div class="graph-container">
      <div class="main-graph">
        <graph-container
          :data="graphData"
          :loading="loading"
          :error="error"
          @node-click="handleNodeClick"
          @edge-click="handleEdgeClick"
          @retry="fetchLineageGraph"
        />
      </div>
      
      <info-panel
        :selected-item="selectedItem"
        :item-type="selectedItemType"
        :loading="detailsLoading"
        :error="detailsError"
        :graph-data="graphData"
        @expand-neighbors="handleExpandNeighbors"
      />
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, reactive, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import GraphContainer from '@/components/graph/GraphContainer.vue';
import InfoPanel from '@/components/panels/InfoPanel.vue';
import { lineageApi } from '@/services/api';
import { NodeType, GraphQueryParams } from '@/types/api';
import { ElMessage } from 'element-plus';

// 路由实例
const route = useRoute();
const router = useRouter();

// 查询参数
const queryParams = reactive<GraphQueryParams>({
  root_node_type: NodeType.TABLE,
  root_node_fqn: '',
  depth: 2
});

// 节点类型选项
const nodeTypeOptions = [
  { value: NodeType.DATABASE, label: '数据库' },
  { value: NodeType.SCHEMA, label: '模式' },
  { value: NodeType.TABLE, label: '表' },
  { value: NodeType.VIEW, label: '视图' },
  { value: NodeType.COLUMN, label: '列' },
  { value: NodeType.SQL_PATTERN, label: 'SQL模式' },
  { value: NodeType.FUNCTION, label: '函数' }
];

// 图数据
const graphData = ref<{ nodes: any[]; edges: any[] } | undefined>(undefined);
const loading = ref(false);
const error = ref<string | undefined>(undefined);

// 选中项
const selectedItem = ref<any | undefined>(undefined);
const selectedItemType = ref<'node' | 'edge' | undefined>(undefined);
const detailsLoading = ref(false);
const detailsError = ref<string | undefined>(undefined);

// 获取血缘图数据
const fetchLineageGraph = async () => {
  // 验证参数
  if (!queryParams.root_node_fqn) {
    ElMessage.warning('请输入对象全限定名');
    return;
  }
  
  loading.value = true;
  error.value = undefined;
  
  try {
    const response = await lineageApi.getLineageGraph(queryParams);
    console.log('血缘图数据:', response);
    console.log('节点数量:', response.nodes?.length || 0);
    console.log('边数量:', response.edges?.length || 0);
    
    // 如果节点或边为空，记录详细信息
    if (!response.nodes || response.nodes.length === 0) {
      console.warn('未接收到节点数据');
    } else {
      console.log('节点示例:', response.nodes[0]);
    }
    
    if (!response.edges || response.edges.length === 0) {
      console.warn('未接收到边数据');
    } else {
      console.log('边示例:', response.edges[0]);
    }
    
    graphData.value = response;
    
    // 更新URL查询参数，但不触发路由变化
    router.replace({
      query: {
        type: queryParams.root_node_type,
        fqn: queryParams.root_node_fqn,
        depth: queryParams.depth ? queryParams.depth.toString() : '1'
      }
    });
  } catch (err: any) {
    error.value = err.message || '获取血缘图失败';
    console.error('获取血缘图失败:', err);
  } finally {
    loading.value = false;
  }
};

// 重置查询
const resetQuery = () => {
  queryParams.root_node_type = NodeType.TABLE;
  queryParams.root_node_fqn = '';
  queryParams.depth = 2;
};

// 处理节点点击
const handleNodeClick = (node: any) => {
  selectedItem.value = node;
  selectedItemType.value = 'node';
};

// 处理边点击
const handleEdgeClick = (edge: any) => {
  selectedItem.value = edge;
  selectedItemType.value = 'edge';
};

// 处理展开邻居
const handleExpandNeighbors = async (nodeId: string) => {
  if (!graphData.value) return;
  
  // 查找节点
  const node = graphData.value.nodes.find(n => n.id === nodeId);
  if (!node) return;
  
  // 更新查询参数
  queryParams.root_node_type = node.type;
  queryParams.root_node_fqn = node.fqn || node.id;
  
  // 获取血缘图
  await fetchLineageGraph();
};

// 从URL查询参数初始化
const initFromQuery = () => {
  const { type, fqn, depth } = route.query;
  
  if (type && typeof type === 'string' && Object.values(NodeType).includes(type as NodeType)) {
    queryParams.root_node_type = type as NodeType;
  }
  
  if (fqn && typeof fqn === 'string') {
    queryParams.root_node_fqn = fqn;
  }
  
  if (depth && typeof depth === 'string') {
    const depthNum = parseInt(depth, 10);
    if (!isNaN(depthNum) && depthNum >= 1 && depthNum <= 5) {
      queryParams.depth = depthNum;
    }
  }
  
  // 如果有足够的参数，自动查询
  if (queryParams.root_node_fqn) {
    fetchLineageGraph();
  }
};

// 监听路由变化
watch(
  () => route.query,
  () => {
    initFromQuery();
  }
);

// 组件挂载时初始化
onMounted(() => {
  initFromQuery();
});
</script>

<style lang="scss" scoped>
.lineage-graph-page {
  height: 100%;
  display: flex;
  flex-direction: column;
  
  .page-header {
    padding-bottom: 20px;
    
    h2 {
      margin: 0 0 15px;
      font-size: 20px;
      font-weight: 600;
      color: var(--text-color);
    }
    
    .search-filters {
      background-color: #fff;
      border-radius: 4px;
      padding: 15px;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
      
      .filter-form {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        
        .el-form-item {
          margin-bottom: 0;
          margin-right: 20px;
        }
      }
    }
  }
  
  .graph-container {
    flex: 1;
    display: flex;
    overflow: hidden;
    
    .main-graph {
      flex: 1;
      overflow: hidden;
    }
  }
}
</style>
