<template>
  <div class="object-details-page">
    <div class="page-header">
      <el-page-header @back="goBack">
        <template #content>
          <div class="header-content">
            <span class="object-type-badge" :style="{ backgroundColor: getNodeColor(objectType) }">
              {{ getNodeIcon(objectType) }}
            </span>
            <span class="object-title">{{ objectData?.node?.label || '对象详情' }}</span>
          </div>
        </template>
      </el-page-header>
    </div>
    
    <div v-if="loading" class="loading-container">
      <el-spinner size="large" />
      <p>加载对象详情中...</p>
    </div>
    
    <div v-else-if="error" class="error-container">
      <el-alert
        title="加载对象详情失败"
        type="error"
        :description="error"
        show-icon
        :closable="false"
      />
      <el-button type="primary" @click="fetchObjectDetails" class="retry-button">重试</el-button>
    </div>
    
    <template v-else-if="objectData">
      <div class="object-info-section">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="ID" :span="2">
            {{ objectData.node.id }}
          </el-descriptions-item>
          <el-descriptions-item label="名称">
            {{ objectData.node.label }}
          </el-descriptions-item>
          <el-descriptions-item label="类型">
            {{ getNodeTypeLabel(objectData.node.type) }}
          </el-descriptions-item>
          <el-descriptions-item label="全限定名" :span="2">
            {{ objectData.node.fqn || '-' }}
          </el-descriptions-item>
        </el-descriptions>
        
        <template v-if="Object.keys(objectData.node.properties || {}).length > 0">
          <h3 class="section-title">属性</h3>
          <el-descriptions :column="2" border>
            <el-descriptions-item 
              v-for="(value, key) in objectData.node.properties" 
              :key="key" 
              :label="formatPropertyKey(key)"
              :span="isLongValue(value) ? 2 : 1"
            >
              {{ formatPropertyValue(value) }}
            </el-descriptions-item>
          </el-descriptions>
        </template>
      </div>
      
      <div v-if="objectData.related_objects" class="related-objects-section">
        <h3 class="section-title">相关对象</h3>
        
        <el-tabs type="border-card">
          <el-tab-pane label="血缘图">
            <div class="graph-container">
              <graph-container
                :data="objectData.related_objects"
                @node-click="handleNodeClick"
                @edge-click="handleEdgeClick"
              />
            </div>
          </el-tab-pane>
          
          <el-tab-pane label="上游对象">
            <el-table
              v-if="upstreamObjects.length > 0"
              :data="upstreamObjects"
              style="width: 100%"
              border
            >
              <el-table-column prop="type" label="类型" width="120">
                <template #default="{ row }">
                  {{ getNodeTypeLabel(row.type) }}
                </template>
              </el-table-column>
              <el-table-column prop="label" label="名称" />
              <el-table-column prop="fqn" label="全限定名" />
              <el-table-column label="操作" width="120">
                <template #default="{ row }">
                  <el-button type="primary" link @click="navigateToObject(row)">查看</el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-else description="没有上游对象" />
          </el-tab-pane>
          
          <el-tab-pane label="下游对象">
            <el-table
              v-if="downstreamObjects.length > 0"
              :data="downstreamObjects"
              style="width: 100%"
              border
            >
              <el-table-column prop="type" label="类型" width="120">
                <template #default="{ row }">
                  {{ getNodeTypeLabel(row.type) }}
                </template>
              </el-table-column>
              <el-table-column prop="label" label="名称" />
              <el-table-column prop="fqn" label="全限定名" />
              <el-table-column label="操作" width="120">
                <template #default="{ row }">
                  <el-button type="primary" link @click="navigateToObject(row)">查看</el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-else description="没有下游对象" />
          </el-tab-pane>
          
          <el-tab-pane label="关系列表">
            <el-table
              v-if="objectData.related_objects.edges.length > 0"
              :data="objectData.related_objects.edges"
              style="width: 100%"
              border
            >
              <el-table-column prop="type" label="关系类型" width="120">
                <template #default="{ row }">
                  {{ getEdgeTypeLabel(row.type) }}
                </template>
              </el-table-column>
              <el-table-column label="源节点">
                <template #default="{ row }">
                  {{ getNodeLabel(row.source) }}
                </template>
              </el-table-column>
              <el-table-column label="目标节点">
                <template #default="{ row }">
                  {{ getNodeLabel(row.target) }}
                </template>
              </el-table-column>
              <el-table-column label="详情" width="80">
                <template #default="{ row }">
                  <el-button type="primary" link @click="showEdgeDetails(row)">详情</el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-else description="没有关系数据" />
          </el-tab-pane>
        </el-tabs>
      </div>
    </template>
    
    <el-dialog
      v-model="edgeDetailsVisible"
      title="关系详情"
      width="600px"
    >
      <template v-if="selectedEdge">
        <el-descriptions :column="1" border>
          <el-descriptions-item label="关系类型">
            {{ getEdgeTypeLabel(selectedEdge.type) }}
          </el-descriptions-item>
          <el-descriptions-item label="源节点">
            {{ getNodeLabel(selectedEdge.source) }}
          </el-descriptions-item>
          <el-descriptions-item label="目标节点">
            {{ getNodeLabel(selectedEdge.target) }}
          </el-descriptions-item>
        </el-descriptions>
        
        <template v-if="Object.keys(selectedEdge.properties || {}).length > 0">
          <h4 class="dialog-section-title">属性</h4>
          <el-descriptions :column="1" border>
            <el-descriptions-item 
              v-for="(value, key) in selectedEdge.properties" 
              :key="key" 
              :label="formatPropertyKey(key)"
            >
              {{ formatPropertyValue(value) }}
            </el-descriptions-item>
          </el-descriptions>
        </template>
      </template>
    </el-dialog>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import GraphContainer from '@/components/graph/GraphContainer.vue';
import { lineageApi } from '@/services/api';
import { NodeType, EdgeType, ObjectDetailsResponse } from '@/types/api';
import { NODE_STYLE_MAP, EDGE_STYLE_MAP } from '@/types/graph';

// 路由实例
const route = useRoute();
const router = useRouter();

// 对象类型和FQN
const objectType = computed(() => route.params.type as NodeType);
const objectFqn = computed(() => decodeURIComponent(route.params.fqn as string));

// 对象数据
const objectData = ref<ObjectDetailsResponse | null>(null);
const loading = ref(false);
const error = ref<string | null>(null);

// 边详情对话框
const edgeDetailsVisible = ref(false);
const selectedEdge = ref<any | null>(null);

// 上游和下游对象
const upstreamObjects = computed(() => {
  if (!objectData.value?.related_objects) return [];
  if (!objectData.value.related_objects.edges || !Array.isArray(objectData.value.related_objects.edges)) return [];
  
  const nodeId = objectData.value.node.id;
  const incomingEdges = objectData.value.related_objects.edges.filter(edge => edge.target === nodeId);
  
  if (!incomingEdges || !Array.isArray(incomingEdges)) return [];
  
  return incomingEdges.map(edge => {
    if (!objectData.value?.related_objects?.nodes || !Array.isArray(objectData.value.related_objects.nodes)) {
      return { id: edge.source, label: String(edge.source), type: NodeType.TABLE };
    }
    const sourceNode = objectData.value.related_objects.nodes.find(node => node.id === edge.source);
    return sourceNode || { id: edge.source, label: String(edge.source), type: NodeType.TABLE };
  });
});

const downstreamObjects = computed(() => {
  if (!objectData.value?.related_objects) return [];
  if (!objectData.value.related_objects.edges || !Array.isArray(objectData.value.related_objects.edges)) return [];
  
  const nodeId = objectData.value.node.id;
  const outgoingEdges = objectData.value.related_objects.edges.filter(edge => edge.source === nodeId);
  
  if (!outgoingEdges || !Array.isArray(outgoingEdges)) return [];
  
  return outgoingEdges.map(edge => {
    if (!objectData.value?.related_objects?.nodes || !Array.isArray(objectData.value.related_objects.nodes)) {
      return { id: edge.target, label: String(edge.target), type: NodeType.TABLE };
    }
    const targetNode = objectData.value.related_objects.nodes.find(node => node.id === edge.target);
    return targetNode || { id: edge.target, label: String(edge.target), type: NodeType.TABLE };
  });
});

// 获取对象详情
const fetchObjectDetails = async () => {
  loading.value = true;
  error.value = null;
  
  try {
    const response = await lineageApi.getObjectDetails({
      node_type: objectType.value,
      node_fqn: objectFqn.value,
      include_related: true
    });
    
    objectData.value = response;
  } catch (err: any) {
    error.value = err.message || '获取对象详情失败';
    console.error('获取对象详情失败:', err);
  } finally {
    loading.value = false;
  }
};

// 返回上一页
const goBack = () => {
  router.back();
};

// 处理节点点击
const handleNodeClick = (node: any) => {
  navigateToObject(node);
};

// 处理边点击
const handleEdgeClick = (edge: any) => {
  showEdgeDetails(edge);
};

// 导航到对象
const navigateToObject = (node: any) => {
  router.push({
    name: 'object-details',
    params: {
      type: node.type,
      fqn: encodeURIComponent(node.fqn || node.id)
    }
  });
};

// 显示边详情
const showEdgeDetails = (edge: any) => {
  selectedEdge.value = edge;
  edgeDetailsVisible.value = true;
};

// 获取节点颜色
const getNodeColor = (type: NodeType) => {
  return NODE_STYLE_MAP[type]?.color || '#5B8FF9';
};

// 获取节点图标
const getNodeIcon = (type: NodeType) => {
  switch (type) {
    case NodeType.DATABASE:
      return '🗄️';
    case NodeType.SCHEMA:
      return '📁';
    case NodeType.TABLE:
      return '📋';
    case NodeType.VIEW:
      return '👁️';
    case NodeType.COLUMN:
      return '📊';
    case NodeType.SQL_PATTERN:
      return '⚙️';
    case NodeType.FUNCTION:
      return '🔧';
    default:
      return '📄';
  }
};

// 获取节点类型标签
const getNodeTypeLabel = (type: NodeType) => {
  switch (type) {
    case NodeType.DATABASE:
      return '数据库';
    case NodeType.SCHEMA:
      return '模式';
    case NodeType.TABLE:
      return '表';
    case NodeType.VIEW:
      return '视图';
    case NodeType.COLUMN:
      return '列';
    case NodeType.SQL_PATTERN:
      return 'SQL模式';
    case NodeType.FUNCTION:
      return '函数';
    default:
      return '未知类型';
  }
};

// 获取边类型标签
const getEdgeTypeLabel = (type: EdgeType) => {
  switch (type) {
    case EdgeType.CONTAINS:
      return '包含';
    case EdgeType.REFERENCES:
      return '引用';
    case EdgeType.DEPENDS_ON:
      return '依赖';
    case EdgeType.DATA_FLOW:
      return '数据流';
    case EdgeType.GENERATES_FLOW:
      return '生成流';
    case EdgeType.WRITES:
      return '写入';
    case EdgeType.READS:
      return '读取';
    default:
      return '未知关系';
  }
};

// 获取节点标签
const getNodeLabel = (nodeId: string) => {
  if (!objectData.value?.related_objects) return nodeId;
  
  const node = objectData.value.related_objects.nodes.find(node => node.id === nodeId);
  return node ? node.label : nodeId;
};

// 格式化属性键
const formatPropertyKey = (key: string) => {
  return key
    .replace(/_/g, ' ')
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, str => str.toUpperCase());
};

// 格式化属性值
const formatPropertyValue = (value: any) => {
  if (value === null || value === undefined) {
    return '-';
  }
  
  if (typeof value === 'object') {
    return JSON.stringify(value, null, 2);
  }
  
  return String(value);
};

// 判断是否为长值
const isLongValue = (value: any) => {
  if (typeof value === 'object') {
    return true;
  }
  
  if (typeof value === 'string' && value.length > 50) {
    return true;
  }
  
  return false;
};

// 监听路由参数变化
watch(
  [objectType, objectFqn],
  () => {
    fetchObjectDetails();
  }
);

// 组件挂载时获取对象详情
onMounted(() => {
  fetchObjectDetails();
});
</script>

<style lang="scss" scoped>
.object-details-page {
  .page-header {
    margin-bottom: 20px;
    
    .header-content {
      display: flex;
      align-items: center;
      
      .object-type-badge {
        width: 30px;
        height: 30px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        margin-right: 10px;
        color: #fff;
        font-size: 16px;
      }
      
      .object-title {
        font-size: 18px;
        font-weight: 600;
      }
    }
  }
  
  .loading-container,
  .error-container {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 300px;
    
    p {
      margin-top: 10px;
      color: #666;
    }
    
    .retry-button {
      margin-top: 20px;
    }
  }
  
  .object-info-section {
    margin-bottom: 30px;
  }
  
  .section-title {
    margin: 20px 0 10px;
    font-size: 18px;
    font-weight: 600;
    color: var(--text-color);
  }
  
  .graph-container {
    height: 500px;
    border: 1px solid var(--border-color);
    border-radius: 4px;
    overflow: hidden;
  }
  
  .dialog-section-title {
    margin: 20px 0 10px;
    font-size: 16px;
    font-weight: 600;
    color: var(--text-color);
  }
}
</style>
