/**
 * API 服务
 * 
 * 此文件提供与后端 API 交互的服务函数
 * 
 * 作者: Vance Chen
 */

import axios from 'axios';
import type { 
  GraphResponse, 
  ObjectDetailsResponse, 
  PathResponse,
  ObjectsResponse,
  GraphQueryParams,
  ObjectDetailsQueryParams,
  PathQueryParams,
  ObjectsQueryParams
} from '@/types/api';

// 创建 axios 实例
const apiClient = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  }
});

// 请求拦截器
apiClient.interceptors.request.use(
  config => {
    // 在发送请求前做些什么
    return config;
  },
  error => {
    // 对请求错误做些什么
    console.error('请求错误:', error);
    return Promise.reject(error);
  }
);

// 响应拦截器
apiClient.interceptors.response.use(
  response => {
    // 对响应数据做些什么
    return response.data;
  },
  error => {
    // 对响应错误做些什么
    console.error('响应错误:', error);
    if (error.response) {
      // 服务器返回错误状态码
      console.error('错误状态码:', error.response.status);
      console.error('错误信息:', error.response.data);
    } else if (error.request) {
      // 请求已发送但未收到响应
      console.error('未收到响应');
    } else {
      // 请求配置出错
      console.error('请求配置错误:', error.message);
    }
    return Promise.reject(error);
  }
);

/**
 * 血缘关系 API 服务
 */
export const lineageApi = {
  /**
   * 获取血缘子图
   * 
   * @param params 查询参数
   * @returns 血缘子图数据
   */
  async getLineageGraph(params: GraphQueryParams): Promise<GraphResponse> {
    try {
      return await apiClient.get('/lineage/graph', { params }) as GraphResponse;
    } catch (error) {
      console.error('获取血缘子图失败:', error);
      throw error;
    }
  },

  /**
   * 获取对象详情
   * 
   * @param params 查询参数
   * @returns 对象详情数据
   */
  async getObjectDetails(params: ObjectDetailsQueryParams): Promise<ObjectDetailsResponse> {
    try {
      return await apiClient.get('/lineage/object_details', { params }) as ObjectDetailsResponse;
    } catch (error) {
      console.error('获取对象详情失败:', error);
      throw error;
    }
  },

  /**
   * 查找两点间路径
   * 
   * @param params 查询参数
   * @returns 路径数据
   */
  async findPaths(params: PathQueryParams): Promise<PathResponse> {
    try {
      return await apiClient.get('/lineage/paths', { params }) as PathResponse;
    } catch (error) {
      console.error('查找路径失败:', error);
      throw error;
    }
  },
  
  /**
   * 获取对象列表
   * 
   * @param params 查询参数
   * @returns 对象列表数据
   */
  async getObjects(params: ObjectsQueryParams): Promise<ObjectsResponse> {
    try {
      return await apiClient.get('/lineage/objects', { params }) as ObjectsResponse;
    } catch (error) {
      console.error('获取对象列表失败:', error);
      throw error;
    }
  }
};
