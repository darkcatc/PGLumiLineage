<template>
  <div class="path-finder-page">
    <div class="page-header">
      <h2>路径查找</h2>
      <p class="description">查找两个对象之间的数据流路径，了解数据如何从源对象流向目标对象</p>
      
      <div class="search-form">
        <el-form :model="queryParams" label-position="top">
          <el-row :gutter="20">
            <el-col :span="10">
              <el-form-item label="源对象全限定名" required>
                <el-input 
                  v-model="queryParams.source_node_fqn" 
                  placeholder="例如: tpcds.public.store_sales.ss_sold_date_sk"
                  clearable
                />
              </el-form-item>
            </el-col>
            
            <el-col :span="10">
              <el-form-item label="目标对象全限定名" required>
                <el-input 
                  v-model="queryParams.target_node_fqn" 
                  placeholder="例如: tpcds.public.sales_summary.date_id"
                  clearable
                />
              </el-form-item>
            </el-col>
            
            <el-col :span="4">
              <el-form-item label="最大深度">
                <el-slider 
                  v-model="queryParams.max_depth" 
                  :min="1" 
                  :max="10" 
                  :step="1"
                  :marks="{1: '1', 5: '5', 10: '10'}"
                />
              </el-form-item>
            </el-col>
          </el-row>
          
          <el-form-item>
            <el-button type="primary" @click="findPaths" :loading="loading">查找路径</el-button>
            <el-button @click="resetQuery">重置</el-button>
          </el-form-item>
        </el-form>
      </div>
    </div>
    
    <div v-if="loading" class="loading-container">
      <el-spinner size="large" />
      <p>正在查找路径...</p>
    </div>
    
    <div v-else-if="error" class="error-container">
      <el-alert
        title="查找路径失败"
        type="error"
        :description="error"
        show-icon
        :closable="false"
      />
      <el-button type="primary" @click="findPaths" class="retry-button">重试</el-button>
    </div>
    
    <div v-else-if="pathsData && pathsData.paths.length > 0" class="paths-container">
      <div class="paths-header">
        <h3>找到 {{ pathsData.paths.length }} 条路径</h3>
        <p>从 <strong>{{ getNodeName(queryParams.source_node_fqn) }}</strong> 到 <strong>{{ getNodeName(queryParams.target_node_fqn) }}</strong></p>
      </div>
      
      <el-tabs type="border-card" v-model="activePathIndex">
        <el-tab-pane
          v-for="(path, index) in pathsData.paths"
          :key="index"
          :label="`路径 ${index + 1} (${path.nodes.length} 节点, ${path.edges.length} 边)`"
          :name="String(index)"
        >
          <div class="path-details">
            <div class="path-summary">
              <p>路径长度: {{ path.edges.length }} 步</p>
              <p>节点数量: {{ path.nodes.length }}</p>
            </div>
            
            <div class="path-graph">
              <graph-container
                :data="path"
                @node-click="handleNodeClick"
                @edge-click="handleEdgeClick"
              />
            </div>
            
            <div class="path-steps">
              <h4>路径步骤</h4>
              <el-steps direction="vertical" :active="path.edges.length">
                <el-step 
                  v-for="(edge, edgeIndex) in getOrderedEdges(path)" 
                  :key="edgeIndex"
                  :title="getNodeLabel(edge.source, path)"
                  :description="getEdgeDescription(edge, path)"
                >
                  <template #icon>
                    <div class="step-icon" :style="{ backgroundColor: getNodeColor(getNodeType(edge.source, path)) }">
                      {{ getNodeIcon(getNodeType(edge.source, path)) }}
                    </div>
                  </template>
                </el-step>
                
                <!-- 最后一个节点 -->
                <el-step 
                  :title="getNodeLabel(queryParams.target_node_fqn, path)"
                >
                  <template #icon>
                    <div class="step-icon" :style="{ backgroundColor: getNodeColor(getNodeType(getTargetNodeId(path), path)) }">
                      {{ getNodeIcon(getNodeType(getTargetNodeId(path), path)) }}
                    </div>
                  </template>
                </el-step>
              </el-steps>
            </div>
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>
    
    <div v-else-if="pathsData && pathsData.paths.length === 0" class="no-paths-container">
      <el-empty description="未找到路径">
        <template #description>
          <p>在指定的最大深度内未找到从源对象到目标对象的路径</p>
          <p>尝试增加最大深度或检查对象名称是否正确</p>
        </template>
        <el-button type="primary" @click="resetQuery">重新查询</el-button>
      </el-empty>
    </div>
    
    <div v-else class="empty-state">
      <el-empty description="请输入源对象和目标对象的全限定名，然后点击查找路径">
        <template #description>
          <p>输入两个对象的全限定名，查找它们之间的数据流路径</p>
          <p>例如：tpcds.public.store_sales.ss_sold_date_sk → tpcds.public.sales_summary.date_id</p>
        </template>
      </el-empty>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, reactive } from 'vue';
import { useRouter } from 'vue-router';
import GraphContainer from '@/components/graph/GraphContainer.vue';
import { lineageApi } from '@/services/api';
import { NodeType, EdgeType, PathResponse, PathQueryParams } from '@/types/api';
import { NODE_STYLE_MAP } from '@/types/graph';
import { ElMessage } from 'element-plus';

// 路由实例
const router = useRouter();

// 查询参数
const queryParams = reactive<PathQueryParams>({
  source_node_fqn: '',
  target_node_fqn: '',
  max_depth: 5
});

// 路径数据
const pathsData = ref<PathResponse | null>(null);
const loading = ref(false);
const error = ref<string | null>(null);
const activePathIndex = ref('0');

// 查找路径
const findPaths = async () => {
  // 验证参数
  if (!queryParams.source_node_fqn || !queryParams.target_node_fqn) {
    ElMessage.warning('请输入源对象和目标对象的全限定名');
    return;
  }
  
  loading.value = true;
  error.value = null;
  
  try {
    const response = await lineageApi.findPaths(queryParams);
    pathsData.value = response;
    activePathIndex.value = '0'; // 重置为第一个路径
  } catch (err: any) {
    error.value = err.message || '查找路径失败';
    console.error('查找路径失败:', err);
  } finally {
    loading.value = false;
  }
};

// 重置查询
const resetQuery = () => {
  queryParams.source_node_fqn = '';
  queryParams.target_node_fqn = '';
  queryParams.max_depth = 5;
  pathsData.value = null;
  error.value = null;
};

// 处理节点点击
const handleNodeClick = (node: any) => {
  router.push({
    name: 'object-details',
    params: {
      type: node.type,
      fqn: encodeURIComponent(node.fqn || node.id)
    }
  });
};

// 处理边点击
const handleEdgeClick = (edge: any) => {
  // 可以实现边详情查看逻辑
  console.log('Edge clicked:', edge);
};

// 获取节点名称（从FQN中提取）
const getNodeName = (fqn: string) => {
  if (!fqn) return '';
  
  const parts = fqn.split('.');
  return parts[parts.length - 1];
};

// 获取节点标签
const getNodeLabel = (nodeId: string, path: any) => {
  if (!path || !path.nodes || !Array.isArray(path.nodes)) return nodeId;
  
  const node = path.nodes.find((n: any) => n.id === nodeId || n.fqn === nodeId);
  return node ? node.label : getNodeName(nodeId);
};

// 获取节点类型
const getNodeType = (nodeId: string, path: any) => {
  if (!path || !path.nodes || !Array.isArray(path.nodes)) return NodeType.TABLE;
  
  const node = path.nodes.find((n: any) => n.id === nodeId || n.fqn === nodeId);
  return node ? node.type : NodeType.TABLE;
};

// 获取目标节点ID
const getTargetNodeId = (path: any) => {
  if (!path || !path.nodes || !Array.isArray(path.nodes)) return '';
  
  const targetNode = path.nodes.find((n: any) => n.fqn === queryParams.target_node_fqn);
  return targetNode ? targetNode.id : '';
};

// 获取有序的边（按照路径顺序）
const getOrderedEdges = (path: any) => {
  if (!path || !path.edges || !Array.isArray(path.edges) || path.edges.length === 0) {
    return [];
  }
  
  // 找到源节点
  const sourceNode = path.nodes.find((n: any) => n.fqn === queryParams.source_node_fqn);
  if (!sourceNode) return path.edges;
  
  const sourceId = sourceNode.id;
  const targetId = getTargetNodeId(path);
  
  // 构建有向图
  const graph: Record<string, string[]> = {};
  path.edges.forEach((edge: any) => {
    if (!graph[edge.source]) {
      graph[edge.source] = [];
    }
    graph[edge.source].push(edge.target);
  });
  
  // 找到从源到目标的路径
  const visited = new Set<string>();
  const pathNodes: string[] = [];
  
  const dfs = (node: string): boolean => {
    if (node === targetId) {
      pathNodes.push(node);
      return true;
    }
    
    if (visited.has(node)) return false;
    visited.add(node);
    pathNodes.push(node);
    
    const neighbors = graph[node] || [];
    for (const neighbor of neighbors) {
      if (dfs(neighbor)) return true;
    }
    
    pathNodes.pop();
    return false;
  };
  
  dfs(sourceId);
  
  // 根据路径节点顺序获取边
  const orderedEdges = [];
  for (let i = 0; i < pathNodes.length - 1; i++) {
    const edge = path.edges.find((e: any) => e.source === pathNodes[i] && e.target === pathNodes[i + 1]);
    if (edge) {
      orderedEdges.push(edge);
    }
  }
  
  return orderedEdges;
};

// 获取边描述
const getEdgeDescription = (edge: any, path: any) => {
  const targetLabel = getNodeLabel(edge.target, path);
  const edgeTypeLabel = getEdgeTypeLabel(edge.type);
  
  return `${edgeTypeLabel} → ${targetLabel}`;
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
      return '关系';
  }
};
</script>

<style lang="scss" scoped>
.path-finder-page {
  .page-header {
    margin-bottom: 20px;
    
    h2 {
      margin: 0 0 10px;
      font-size: 20px;
      font-weight: 600;
      color: var(--text-color);
    }
    
    .description {
      margin: 0 0 20px;
      color: #666;
    }
    
    .search-form {
      background-color: #fff;
      border-radius: 4px;
      padding: 20px;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
  }
  
  .loading-container,
  .error-container,
  .empty-state {
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
  
  .paths-container {
    .paths-header {
      margin-bottom: 20px;
      
      h3 {
        margin: 0 0 10px;
        font-size: 18px;
        font-weight: 600;
        color: var(--text-color);
      }
      
      p {
        margin: 0;
        color: #666;
      }
    }
    
    .path-details {
      .path-summary {
        margin-bottom: 20px;
        
        p {
          margin: 5px 0;
          color: #666;
        }
      }
      
      .path-graph {
        height: 400px;
        border: 1px solid var(--border-color);
        border-radius: 4px;
        overflow: hidden;
        margin-bottom: 20px;
      }
      
      .path-steps {
        h4 {
          margin: 0 0 15px;
          font-size: 16px;
          font-weight: 600;
          color: var(--text-color);
        }
        
        .step-icon {
          width: 24px;
          height: 24px;
          border-radius: 50%;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #fff;
          font-size: 14px;
        }
      }
    }
  }
  
  .no-paths-container {
    margin-top: 40px;
    
    p {
      margin: 5px 0;
      color: #666;
    }
  }
}
</style>
