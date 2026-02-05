import React, { useState, useEffect } from 'react';
<<<<<<< HEAD
import { LayoutDashboard, Plus, Play, AlertCircle, Loader2, Search, Trash2, Info, X, ChevronLeft, ChevronRight } from 'lucide-react';
import { getTasks, createTask, deleteTask, deleteTasks, runTask, getDataSources, getAuditLogs, getDataSourceMetadata } from '../api';
=======
import { Terminal, Plus, Play, AlertCircle, Loader2, Search, Trash2, Info, X } from 'lucide-react';
import { getTasks, createTask, deleteTask, runTask, getDataSources, getAuditLogs, getDataSourceMetadata } from '../api';
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98
import { Modal, StatusBadge } from '../components/Common';

const TasksPage = () => {
  const [tasks, setTasks] = useState([]);
  const [sources, setSources] = useState([]);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isDetailModalOpen, setIsDetailModalOpen] = useState(false);
  const [selectedTask, setSelectedTask] = useState(null);
  const [taskErrors, setTaskErrors] = useState({}); // Map task_id to error message
  const [searchName, setSearchName] = useState('');
  const [formData, setFormData] = useState({ 
    name: '', 
    task_type: 'preprocess', 
    config: '' // Will be populated based on type
  });
  
  // Pagination & Selection State
  const [page, setPage] = useState(1);
  const [pageSize] = useState(10);
  const [total, setTotal] = useState(0);
  const [selectedIds, setSelectedIds] = useState([]);
  
  // Sync Task specific state
  const [syncDetails, setSyncDetails] = useState({
      sourceId: '',
      sourceTable: '',
      targetTable: '',
      mode: 'append'
  });
  const [sourceTables, setSourceTables] = useState([]);
  const [isLoadingMetadata, setIsLoadingMetadata] = useState(false);

  useEffect(() => {
    if (syncDetails.sourceId) {
        setIsLoadingMetadata(true);
        getDataSourceMetadata(syncDetails.sourceId)
            .then(res => setSourceTables(res.data.tables || []))
            .catch(err => console.error(err))
            .finally(() => setIsLoadingMetadata(false));
    } else {
        setSourceTables([]);
    }
  }, [syncDetails.sourceId]);

  const fetchTasks = async () => {
    try {
<<<<<<< HEAD
      const params = {
          skip: (page - 1) * pageSize,
          limit: pageSize
      };
      if (searchName) params.name = searchName;
      const res = await getTasks(params);
      
      const items = res.data.items || res.data;
      const totalCount = res.data.total || (Array.isArray(res.data) ? res.data.length : 0);
      
      setTasks(items);
      setTotal(totalCount);
      setSelectedIds([]); // Clear selection on refresh
      
=======
      const params = {};
      if (searchName) params.name = searchName;
      const res = await getTasks(params);
      setTasks(res.data);
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98
      // Fetch error details for failed tasks
      const failedTasks = items.filter(t => t.status === 'failed');
      failedTasks.forEach(t => fetchTaskError(t.id, t.name));
    } catch (err) {
      console.error(err);
    }
  };

  const fetchTaskError = async (taskId, taskName) => {
      try {
          // Look up audit logs for this task failure
          const res = await getAuditLogs({ action: 'task_failed', limit: 5 });
          // Simple matching strategy: find log where resource == taskName
          // Response structure changed to { items: [], total: ... }
          const logs = res.data.items || res.data; 
          const errorLog = Array.isArray(logs) ? logs.find(l => l.resource === taskName) : null;
          
          if (errorLog) {
              setTaskErrors(prev => ({...prev, [taskId]: errorLog.details}));
          }
      } catch (err) {
          console.error(err);
      }
  };

  const fetchSources = async () => {
    try {
      const res = await getDataSources();
      setSources(res.data.data || []);
    } catch (err) {
        console.error(err);
    }
  }

  useEffect(() => {
    fetchTasks();
    fetchSources();
    const interval = setInterval(fetchTasks, 5000); // Poll every 5s
    return () => clearInterval(interval);
<<<<<<< HEAD
  }, [searchName, page, pageSize]); // Add pagination dependencies
=======
  }, [searchName]);
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98

  // Update default config when task type changes
  useEffect(() => {
      if (formData.task_type === 'preprocess') {
          setFormData(prev => ({
              ...prev,
              config: JSON.stringify({
                job_name: "MyJob",
                source: { type: "csv", path: "data/input.csv" },
                operators: [{ type: "dedup" }],
                target: { type: "csv", path: "data/output", mode: "overwrite" }
              }, null, 2)
          }));
      } else if (formData.task_type === 'sync') {
           // Clear manual config, we'll build it from UI
           setFormData(prev => ({ ...prev, config: '' }));
      }
  }, [formData.task_type]);

  const handleRun = async (id) => {
    try {
      await runTask(id);
      fetchTasks();
    } catch (err) {
      alert('Failed to start task');
    }
  };

  const handleDelete = async (id) => {
      if (confirm('确认删除此任务?')) {
          try {
              await deleteTask(id);
              fetchTasks();
          } catch (err) {
              alert('删除失败');
              console.error(err);
          }
      }
  };

<<<<<<< HEAD
  const handleBulkDelete = async () => {
      if (selectedIds.length === 0) return;
      
      if (confirm(`确认删除选中的 ${selectedIds.length} 个任务?`)) {
          try {
              await deleteTasks(selectedIds);
              fetchTasks();
          } catch (err) {
              alert('删除失败');
              console.error(err);
          }
      }
  };

  const handleSelectAll = (e) => {
      if (e.target.checked) {
          setSelectedIds(tasks.map(task => task.id));
      } else {
          setSelectedIds([]);
      }
  };

  const handleSelectOne = (id) => {
      if (selectedIds.includes(id)) {
          setSelectedIds(selectedIds.filter(sid => sid !== id));
      } else {
          setSelectedIds([...selectedIds, id]);
      }
  };

=======
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98
  const openDetailModal = (task) => {
      setSelectedTask(task);
      setIsDetailModalOpen(true);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      let finalConfig = formData.config;
      
      // If sync task, build config from UI
      if (formData.task_type === 'sync') {
          if (!syncDetails.sourceId || !syncDetails.sourceTable || !syncDetails.targetTable) {
              alert("Please complete all sync fields");
              return;
          }
          
          const configObj = {
              source_id: parseInt(syncDetails.sourceId),
              source: {
                  table: syncDetails.sourceTable
              },
              target: {
                  table: syncDetails.targetTable,
                  mode: syncDetails.mode
              }
          };
          finalConfig = JSON.stringify(configObj);
      }

      await createTask({
          ...formData,
          config: finalConfig
      });
      setIsModalOpen(false);
      fetchTasks();
    } catch (err) {
      alert('Failed to create task');
    }
  };

  const totalPages = Math.ceil(total / pageSize);

  const [jumpPage, setJumpPage] = useState('');

  const handleJump = () => {
      const p = parseInt(jumpPage);
      if (!isNaN(p) && p >= 1 && p <= totalPages) {
          setPage(p);
          setJumpPage('');
      }
  };

  return (
    <div className="space-y-6 flex flex-col h-[calc(100vh-8rem)]">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-slate-800 flex items-center gap-2">
          <LayoutDashboard className="text-emerald-500" /> 任务管理
        </h2>
        <div className="flex gap-2">
            {selectedIds.length > 0 && (
                <button 
                    onClick={handleBulkDelete}
                    className="bg-rose-50 border border-rose-200 hover:bg-rose-100 text-rose-600 px-4 py-2 rounded flex items-center gap-2 transition-colors"
                >
                    <Trash2 size={16} /> 删除选中 ({selectedIds.length})
                </button>
            )}
            <button 
              onClick={() => setIsModalOpen(true)}
              className="bg-emerald-600 hover:bg-emerald-500 text-white px-4 py-2 rounded-md flex items-center gap-2 transition-colors shadow-sm"
            >
              <Plus size={18} /> 创建任务
            </button>
        </div>
      </div>

<<<<<<< HEAD
      <div className="bg-white border border-slate-200 rounded-lg p-4 flex gap-4 items-center shadow-sm">
          <div className="relative flex-1 max-w-xs">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={16} />
              <input 
                  type="text" 
                  placeholder="按名称搜索..." 
                  className="w-full bg-slate-50 border border-slate-200 rounded-md pl-9 pr-4 py-2 text-sm text-slate-700 focus:outline-none focus:border-emerald-500 focus:ring-1 focus:ring-emerald-500/20"
                  value={searchName}
                  onChange={e => {
                      setSearchName(e.target.value);
                      setPage(1); // Reset page on search
                  }}
=======
      <div className="bg-slate-900 border border-slate-700 rounded-lg p-4 flex gap-4 items-center">
          <div className="relative flex-1 max-w-xs">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" size={16} />
              <input 
                  type="text" 
                  placeholder="按名称搜索..." 
                  className="w-full bg-slate-950 border border-slate-700 rounded-md pl-9 pr-4 py-2 text-sm text-slate-200 focus:outline-none focus:border-emerald-500"
                  value={searchName}
                  onChange={e => setSearchName(e.target.value)}
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98
              />
          </div>
      </div>

<<<<<<< HEAD
      <div className="bg-white border border-slate-200 rounded-lg overflow-hidden shadow-sm flex-1 overflow-auto">
=======
      <div className="bg-slate-900 border border-slate-700 rounded-lg overflow-hidden">
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-50 border-b border-slate-200 text-slate-500 text-sm uppercase tracking-wider sticky top-0 z-10">
              <th className="p-4 w-10">
                  <input 
                    type="checkbox" 
                    className="rounded border-slate-300 bg-white text-emerald-500 focus:ring-emerald-500/30"
                    checked={tasks.length > 0 && selectedIds.length === tasks.length}
                    onChange={handleSelectAll}
                  />
              </th>
              <th className="p-4 font-medium">任务ID</th>
              <th className="p-4 font-medium">名称</th>
              <th className="p-4 font-medium">类型</th>
              <th className="p-4 font-medium">状态</th>
              <th className="p-4 font-medium text-right">操作</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {tasks.map(task => (
              <tr key={task.id} className="hover:bg-slate-50 transition-colors">
                <td className="p-4">
                    <input 
                        type="checkbox" 
                        className="rounded border-slate-300 bg-white text-emerald-500 focus:ring-emerald-500/30"
                        checked={selectedIds.includes(task.id)}
                        onChange={() => handleSelectOne(task.id)}
                    />
                </td>
                <td className="p-4 text-slate-500 font-mono">#{task.id}</td>
                <td className="p-4 font-medium text-slate-700">{task.name}</td>
                <td className="p-4 text-slate-600">{task.task_type === 'sync' ? '同步' : '预处理'}</td>
                <td className="p-4">
                  <div className="flex flex-col gap-2">
                      <div className="flex items-center gap-2">
                        <div title={taskErrors[task.id] ? `失败原因: ${taskErrors[task.id]}` : ""}>
                            <StatusBadge status={task.status} />
                        </div>
                        {task.status === 'failed' && taskErrors[task.id] && (
                            <div className="relative group cursor-help">
                                <AlertCircle size={16} className="text-rose-500" />
                                <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 hidden group-hover:block w-64 p-2 bg-rose-50 border border-rose-200 rounded shadow-xl text-xs text-rose-600 z-10 break-words">
                                    {taskErrors[task.id]}
                                </div>
                            </div>
                        )}
                      </div>
                      {(task.task_type === 'sync' || task.progress > 0) && task.status !== 'pending' && (
                          <div className="w-full bg-slate-100 rounded-full h-1.5" title={`${task.progress || 0}%`}>
                                <div 
                                    className="bg-emerald-500 h-1.5 rounded-full transition-all duration-500" 
                                    style={{ width: `${task.progress || 0}%` }}
                                ></div>
                          </div>
                      )}
                  </div>
                </td>
                <td className="p-4 text-right">
                  <div className="flex justify-end gap-2">
                    <button 
                        onClick={() => openDetailModal(task)}
<<<<<<< HEAD
                        className="text-slate-400 hover:text-emerald-500 p-2 rounded hover:bg-emerald-50 transition-colors"
=======
                        className="text-slate-500 hover:text-emerald-400 p-2 rounded hover:bg-emerald-950/50 transition-colors"
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98
                        title="查看详情"
                    >
                        <Info size={18} />
                    </button>
                    <button 
                        onClick={() => handleDelete(task.id)}
<<<<<<< HEAD
                        className="text-slate-400 hover:text-rose-500 p-2 rounded hover:bg-rose-50 transition-colors"
=======
                        className="text-slate-500 hover:text-rose-500 p-2 rounded hover:bg-rose-950/50 transition-colors"
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98
                        title="删除任务"
                    >
                        <Trash2 size={18} />
                    </button>
                    <button 
                        onClick={() => handleRun(task.id)}
                        disabled={task.status === 'running'}
<<<<<<< HEAD
                        className="text-emerald-500 hover:text-emerald-600 disabled:opacity-50 disabled:cursor-not-allowed p-2 rounded hover:bg-emerald-50 transition-colors"
=======
                        className="text-emerald-500 hover:text-emerald-400 disabled:opacity-50 disabled:cursor-not-allowed p-2 rounded hover:bg-emerald-950/50 transition-colors"
>>>>>>> 3061eb339f8abec3c0d110549ba36dd5b17e0c98
                        title="运行任务"
                    >
                        <Play size={18} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
            {tasks.length === 0 && (
                <tr>
                    <td colSpan="6" className="p-8 text-center text-slate-400">暂无任务，请点击右上角创建。</td>
                </tr>
            )}
          </tbody>
        </table>
      </div>
      
      {/* Pagination Controls */}
      <div className="mt-auto pt-4 border-t border-slate-200 flex items-center justify-between">
          <div className="text-sm text-slate-500">
             共 {total} 条
          </div>
          <div className="flex gap-2 items-center">
             <div className="flex items-center gap-2 mr-4">
                  <span className="text-sm text-slate-500">前往</span>
                  <input 
                      type="number" 
                      min="1" 
                      max={totalPages}
                      className="w-12 h-8 text-center bg-white border border-slate-200 rounded text-sm text-slate-700 focus:outline-none focus:border-purple-500"
                      value={jumpPage}
                      onChange={e => setJumpPage(e.target.value)}
                      onKeyDown={e => e.key === 'Enter' && handleJump()}
                  />
                  <span className="text-sm text-slate-500">页</span>
              </div>
              <button 
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="w-8 h-8 flex items-center justify-center rounded bg-white border border-slate-200 text-slate-600 hover:text-slate-800 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
              >
                  <ChevronLeft size={16} />
              </button>
              <div className="flex items-center gap-1">
                  {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                      let p = page;
                      if (totalPages > 5) {
                          if (page <= 3) p = i + 1;
                          else if (page >= totalPages - 2) p = totalPages - 4 + i;
                          else p = page - 2 + i;
                      } else {
                          p = i + 1;
                      }
                      
                      return (
                          <button
                              key={p}
                              onClick={() => setPage(p)}
                              className={`w-8 h-8 rounded text-sm font-medium transition-colors ${
                                  page === p 
                                  ? 'bg-purple-600 text-white shadow-sm' 
                                  : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-50'
                              }`}
                          >
                              {p}
                          </button>
                      );
                  })}
              </div>
              <button 
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages}
                  className="w-8 h-8 flex items-center justify-center rounded bg-white border border-slate-200 text-slate-600 hover:text-slate-800 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
              >
                  <ChevronRight size={16} />
              </button>
          </div>
      </div>

      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="创建新任务">
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-400 mb-1">任务名称</label>
            <input 
              type="text" 
              required
              className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
              value={formData.name}
              onChange={e => setFormData({...formData, name: e.target.value})}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-400 mb-1">任务类型</label>
            <select 
              className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
              value={formData.task_type}
              onChange={e => setFormData({...formData, task_type: e.target.value})}
            >
              <option value="preprocess">预处理 (Spark)</option>
              <option value="sync">全量同步</option>
            </select>
          </div>
          
          {formData.task_type === 'sync' ? (
             <div className="space-y-4 p-4 bg-slate-800/50 rounded border border-slate-700">
                 <h4 className="text-sm font-semibold text-slate-300">同步配置</h4>
                 <div>
                    <label className="block text-sm font-medium text-slate-400 mb-1">源数据库</label>
                    <select 
                      required
                      className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
                      value={syncDetails.sourceId}
                      onChange={e => setSyncDetails({...syncDetails, sourceId: e.target.value, sourceTable: ''})}
                    >
                      <option value="">选择数据源...</option>
                      {sources.filter(s => ['mysql', 'clickhouse', 'minio'].includes(s.type)).map(s => (
                          <option key={s.id} value={s.id}>{s.name} ({s.type})</option>
                      ))}
                    </select>
                 </div>
                 
                 {syncDetails.sourceId && (
                     <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">
                            {isLoadingMetadata ? (
                                <span className="flex items-center gap-2">加载表列表中... <Loader2 size={14} className="animate-spin"/></span>
                            ) : (
                                sources.find(s => s.id == syncDetails.sourceId)?.type === 'minio' ? "源 Bucket" : "源表"
                            )}
                        </label>
                        <select 
                          required
                          className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
                          value={syncDetails.sourceTable}
                          onChange={e => setSyncDetails({...syncDetails, sourceTable: e.target.value})}
                          disabled={isLoadingMetadata || sourceTables.length === 0}
                        >
                          <option value="">
                              {sources.find(s => s.id == syncDetails.sourceId)?.type === 'minio' ? "选择 Bucket..." : "选择表..."}
                          </option>
                          {sourceTables.map(t => (
                              <option key={t} value={t}>{t}</option>
                          ))}
                        </select>
                        {sourceTables.length === 0 && !isLoadingMetadata && (
                            <p className="text-xs text-amber-500 mt-1">未找到表或连接失败。</p>
                        )}
                     </div>
                 )}

                 <div>
                    <label className="block text-sm font-medium text-slate-400 mb-1">
                        {sources.find(s => s.id == syncDetails.sourceId)?.type === 'minio' ? "目标 Bucket" : "目标表 (系统数据库)"}
                    </label>
                    <input 
                      type="text" 
                      required
                      placeholder={sources.find(s => s.id == syncDetails.sourceId)?.type === 'minio' ? "例如: processed-data" : "例如: synced_customers"}
                      className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
                      value={syncDetails.targetTable}
                      onChange={e => setSyncDetails({...syncDetails, targetTable: e.target.value})}
                    />
                 </div>
                 <div>
                    <label className="block text-sm font-medium text-slate-400 mb-1">同步模式</label>
                    <select 
                      className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
                      value={syncDetails.mode}
                      onChange={e => setSyncDetails({...syncDetails, mode: e.target.value})}
                    >
                      <option value="append">追加 (Append)</option>
                      <option value="overwrite">覆盖 (Overwrite)</option>
                    </select>
                 </div>
                 <p className="text-xs text-slate-500 mt-2">
                     * 数据将从选定的源同步到内部系统数据库。
                 </p>
             </div>
          ) : (
            <div>
                <label className="block text-sm font-medium text-slate-400 mb-1">配置 (JSON)</label>
                <textarea 
                className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 font-mono text-sm focus:outline-none focus:border-emerald-500 h-48"
                value={formData.config}
                onChange={e => setFormData({...formData, config: e.target.value})}
                />
            </div>
          )}

          <div className="flex justify-end gap-3 mt-6">
            <button type="button" onClick={() => setIsModalOpen(false)} className="px-4 py-2 text-slate-400 hover:text-white">取消</button>
            <button type="submit" className="bg-emerald-600 hover:bg-emerald-500 text-white px-4 py-2 rounded">创建任务</button>
          </div>
        </form>
      </Modal>

      {/* Task Detail Modal */}
      <Modal isOpen={isDetailModalOpen} onClose={() => setIsDetailModalOpen(false)} title="任务详情">
          {selectedTask && (
              <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                      <div>
                          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">任务名称</label>
                          <div className="text-slate-200 font-medium">{selectedTask.name}</div>
                      </div>
                      <div>
                          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">类型</label>
                          <div className="text-slate-200 font-medium">{selectedTask.task_type}</div>
                      </div>
                  </div>
                  
                  <div>
                      <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">状态</label>
                      <div className="flex items-center gap-2">
                        <StatusBadge status={selectedTask.status} />
                        {selectedTask.progress > 0 && <span className="text-slate-400 text-sm">({selectedTask.progress}%)</span>}
                      </div>
                  </div>

                  <div>
                      <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">配置</label>
                      <div className="bg-slate-950 rounded p-3 text-xs font-mono text-slate-400 overflow-auto max-h-48 border border-slate-800">
                         <pre>{JSON.stringify(JSON.parse(selectedTask.config), null, 2)}</pre>
                      </div>
                  </div>

                  {taskErrors[selectedTask.id] && (
                      <div className="bg-rose-900/20 border border-rose-900/50 rounded p-3">
                          <label className="block text-xs font-medium text-rose-500 uppercase tracking-wider mb-1">错误日志</label>
                          <p className="text-rose-300 text-xs break-all">{taskErrors[selectedTask.id]}</p>
                      </div>
                  )}

                  <div className="grid grid-cols-2 gap-4 pt-4 border-t border-slate-800">
                      <div>
                          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">创建时间</label>
                          <div className="text-slate-300 text-sm">{new Date(selectedTask.created_at).toLocaleString()}</div>
                      </div>
                      <div>
                          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">最后更新</label>
                          <div className="text-slate-300 text-sm">{new Date(selectedTask.updated_at).toLocaleString()}</div>
                      </div>
                  </div>
              </div>
          )}
      </Modal>
    </div>
  );
};

export default TasksPage;
