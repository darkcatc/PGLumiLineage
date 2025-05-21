/**
 * 路由配置
 * 
 * 作者: Vance Chen
 */

import { createRouter, createWebHistory } from 'vue-router'

// 路由配置
const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      name: 'home',
      component: () => import('./views/Home.vue'),
      meta: {
        title: '首页 - PGLumiLineage'
      }
    },
    {
      path: '/lineage',
      name: 'lineage',
      component: () => import('./views/LineageGraph.vue'),
      meta: {
        title: '数据血缘 - PGLumiLineage'
      }
    },
    {
      path: '/object/:type/:fqn',
      name: 'object-details',
      component: () => import('./views/ObjectDetails.vue'),
      meta: {
        title: '对象详情 - PGLumiLineage'
      }
    },
    {
      path: '/path-finder',
      name: 'path-finder',
      component: () => import('./views/PathFinder.vue'),
      meta: {
        title: '路径查找 - PGLumiLineage'
      }
    },
    {
      path: '/objects',
      name: 'objects',
      component: () => import('./views/ObjectBrowser.vue'),
      meta: {
        title: '对象浏览器 - PGLumiLineage'
      }
    },
    {
      path: '/:pathMatch(.*)*',
      name: 'not-found',
      component: () => import('./views/NotFound.vue'),
      meta: {
        title: '页面未找到 - PGLumiLineage'
      }
    }
  ]
})

// 路由前置守卫，设置页面标题
router.beforeEach((to, _from, next) => {
  // 设置页面标题
  if (to.meta.title) {
    document.title = to.meta.title as string
  }
  next()
})

export default router
