<template>
  <div class="object-browser-page">
    <div class="page-header">
      <h2>对象浏览器</h2>
      <p class="description">浏览并查询数据库对象，包括数据库、模式、表、视图和列</p>
      
      <div class="filter-form">
        <el-form :model="filterParams" label-position="top" inline>
          <el-form-item label="对象类型">
            <el-select v-model="filterParams.object_type" placeholder="选择对象类型" clearable>
              <el-option label="数据库" value="DATABASE" />
              <el-option label="模式" value="SCHEMA" />
              <el-option label="表" value="TABLE" />
              <el-option label="视图" value="VIEW" />
              <el-option label="列" value="COLUMN" />
            </el-select>
          </el-form-item>
          
          <el-form-item label="搜索关键词">
            <el-input 
              v-model="filterParams.keyword" 
              placeholder="输入名称或描述关键词"
              clearable
              @keyup.enter="searchObjects"
            >
              <template #append>
                <el-button @click="searchObjects">
                  <el-icon><Search /></el-icon>
                </el-button>
              </template>
            </el-input>
          </el-form-item>
          
          <el-form-item label="数据库">
            <el-select 
              v-model="filterParams.database" 
              placeholder="选择数据库" 
              clearable
              :disabled="!databaseOptions.length"
              @change="handleDatabaseChange"
            >
              <el-option 
                v-for="db in databaseOptions" 
                :key="db.value" 
                :label="db.label" 
                :value="db.value" 
              />
            </el-select>
          </el-form-item>
          
          <el-form-item label="模式">
            <el-select 
              v-model="filterParams.schema" 
              placeholder="选择模式" 
              clearable
              :disabled="!schemaOptions.length"
            >
              <el-option 
                v-for="schema in schemaOptions" 
                :key="schema.value" 
                :label="schema.label" 
                :value="schema.value" 
              />
            </el-select>
          </el-form-item>
        </el-form>
      </div>
    </div>
    
    <div v-if="loading" class="loading-container">
      <el-spinner size="large" />
      <p>正在加载对象...</p>
    </div>
    
    <div v-else-if="error" class="error-container">
      <el-alert
        title="加载对象失败"
        type="error"
        :description="error"
        show-icon
        :closable="false"
      />
      <el-button type="primary" @click="searchObjects" class="retry-button">重试</el-button>
    </div>
    
    <div v-else-if="objectsData && objectsData.objects.length > 0" class="objects-container">
      <div class="objects-header">
        <h3>找到 {{ objectsData.total_count }} 个对象</h3>
        <div class="pagination-container">
          <el-pagination
            v-model:current-page="currentPage"
            v-model:page-size="pageSize"
            :page-sizes="[10, 20, 50, 100]"
            layout="total, sizes, prev, pager, next"
            :total="objectsData.total_count"
            @size-change="handleSizeChange"
            @current-change="handleCurrentChange"
          />
        </div>
      </div>
      
      <el-table
        :data="objectsData.objects"
        style="width: 100%"
        border
        stripe
        @row-click="handleRowClick"
      >
        <el-table-column prop="type" label="类型" width="100">
          <template #default="{ row }">
            <el-tag :type="getObjectTypeTagType(row.type)">
              {{ getObjectTypeLabel(row.type) }}
            </el-tag>
          </template>
        </el-table-column>
        
        <el-table-column prop="name" label="名称" min-width="150" show-overflow-tooltip />
        
        <el-table-column prop="fqn" label="全限定名" min-width="250" show-overflow-tooltip />
        
        <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.description || '无描述' }}
          </template>
        </el-table-column>
        
        <el-table-column prop="created_at" label="创建时间" width="180">
          <template #default="{ row }">
            {{ formatDate(row.created_at) }}
          </template>
        </el-table-column>
        
        <el-table-column label="操作" width="150" fixed="right">
          <template #default="{ row }">
            <el-button 
              type="primary" 
              size="small" 
              @click.stop="viewObjectDetails(row)"
            >
              详情
            </el-button>
            
            <el-button 
              type="success" 
              size="small" 
              @click.stop="viewLineage(row)"
            >
              血缘
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>
    
    <div v-else-if="objectsData && objectsData.objects.length === 0" class="no-objects-container">
      <el-empty description="未找到对象">
        <template #description>
          <p>未找到符合条件的对象</p>
          <p>尝试修改筛选条件或清除筛选条件重新查询</p>
        </template>
        <el-button type="primary" @click="resetFilter">清除筛选</el-button>
      </el-empty>
    </div>
    
    <div v-else class="empty-state">
      <el-empty description="请选择对象类型或输入关键词进行查询">
        <template #description>
          <p>选择对象类型、数据库或模式，或输入关键词进行查询</p>
          <p>您也可以不设置任何筛选条件，查询所有对象</p>
        </template>
        <el-button type="primary" @click="searchObjects">查询所有对象</el-button>
      </el-empty>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, reactive, onMounted } from 'vue';
import { useRouter } from 'vue-router';
import { lineageApi } from '@/services/api';
import { NodeType, ObjectsResponse, ObjectsQueryParams } from '@/types/api';
import { formatDate } from '@/utils/format';

// 路由实例
const router = useRouter();

// 筛选参数
const filterParams = reactive<ObjectsQueryParams>({
  object_type: '',
  keyword: '',
  database: '',
  schema: '',
  page: 1,
  page_size: 20
});

// 分页参数
const currentPage = ref(1);
const pageSize = ref(20);

// 对象数据
const objectsData = ref<ObjectsResponse | null>(null);
const loading = ref(false);
const error = ref<string | null>(null);

// 数据库和模式选项
const databaseOptions = ref<{ label: string; value: string }[]>([]);
const schemaOptions = ref<{ label: string; value: string }[]>([]);

// 初始化
onMounted(async () => {
  await loadDatabases();
});

// 加载数据库列表
const loadDatabases = async () => {
  try {
    const response = await lineageApi.getObjects({
      object_type: NodeType.DATABASE,
      page: 1,
      page_size: 100
    });
    
    databaseOptions.value = response.objects.map(db => ({
      label: db.name,
      value: db.name
    }));
  } catch (err) {
    console.error('加载数据库列表失败:', err);
  }
};

// 处理数据库变更
const handleDatabaseChange = async (value: string) => {
  if (!value) {
    schemaOptions.value = [];
    filterParams.schema = '';
    return;
  }
  
  try {
    const response = await lineageApi.getObjects({
      object_type: NodeType.SCHEMA,
      database: value,
      page: 1,
      page_size: 100
    });
    
    schemaOptions.value = response.objects.map(schema => ({
      label: schema.name,
      value: schema.name
    }));
  } catch (err) {
    console.error('加载模式列表失败:', err);
    schemaOptions.value = [];
  }
};

// 搜索对象
const searchObjects = async () => {
  loading.value = true;
  error.value = null;
  
  try {
    // 更新分页参数
    filterParams.page = currentPage.value;
    filterParams.page_size = pageSize.value;
    
    const response = await lineageApi.getObjects(filterParams);
    objectsData.value = response;
  } catch (err: any) {
    error.value = err.message || '加载对象失败';
    console.error('加载对象失败:', err);
  } finally {
    loading.value = false;
  }
};

// 重置筛选条件
const resetFilter = () => {
  filterParams.object_type = '';
  filterParams.keyword = '';
  filterParams.database = '';
  filterParams.schema = '';
  currentPage.value = 1;
  searchObjects();
};

// 处理页码变更
const handleCurrentChange = (page: number) => {
  currentPage.value = page;
  searchObjects();
};

// 处理每页数量变更
const handleSizeChange = (size: number) => {
  pageSize.value = size;
  searchObjects();
};

// 处理行点击
const handleRowClick = (row: any) => {
  viewObjectDetails(row);
};

// 查看对象详情
const viewObjectDetails = (row: any) => {
  router.push({
    name: 'object-details',
    params: {
      type: row.type,
      fqn: encodeURIComponent(row.fqn)
    }
  });
};

// 查看对象血缘
const viewLineage = (row: any) => {
  router.push({
    name: 'lineage',
    query: {
      fqn: row.fqn,
      type: row.type
    }
  });
};

// 获取对象类型标签
const getObjectTypeLabel = (type: NodeType) => {
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
      return '未知';
  }
};

// 获取对象类型标签类型
const getObjectTypeTagType = (type: NodeType) => {
  switch (type) {
    case NodeType.DATABASE:
      return 'danger';
    case NodeType.SCHEMA:
      return 'warning';
    case NodeType.TABLE:
      return 'success';
    case NodeType.VIEW:
      return 'info';
    case NodeType.COLUMN:
      return 'primary';
    case NodeType.SQL_PATTERN:
      return 'danger';
    case NodeType.FUNCTION:
      return 'warning';
    default:
      return 'info';
  }
};
</script>

<style lang="scss" scoped>
.object-browser-page {
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
    
    .filter-form {
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
  
  .objects-container {
    .objects-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 20px;
      
      h3 {
        margin: 0;
        font-size: 18px;
        font-weight: 600;
        color: var(--text-color);
      }
    }
  }
  
  .no-objects-container {
    margin-top: 40px;
    
    p {
      margin: 5px 0;
      color: #666;
    }
  }
}
</style>
