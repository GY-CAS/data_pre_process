import React, { useState } from 'react';
import { LayoutDashboard, Database, Server, Folder, ShieldAlert } from 'lucide-react';
import DataSourcesPage from './pages/DataSourcesPage';
import TasksPage from './pages/TasksPage';
import DataManagementPage from './pages/DataManagementPage';
import AuditPage from './pages/AuditPage';

function App() {
  const [activeTab, setActiveTab] = useState('datasources');

  const renderContent = () => {
    switch(activeTab) {
      case 'datasources': return <DataSourcesPage />;
      case 'datamgmt': return <DataManagementPage />;
      case 'tasks': return <TasksPage />;
      case 'audit': return <AuditPage />;
      default: return <DataSourcesPage />;
    }
  };

  const getTitle = () => {
    switch(activeTab) {
      case 'datasources': return '数据源管理';
      case 'datamgmt': return '数据管理';
      case 'tasks': return '任务管理';
      case 'audit': return '审计日志';
      default: return '数据源管理';
    }
  };

  const NavButton = ({ id, icon: Icon, label }) => (
    <button 
      onClick={() => setActiveTab(id)}
      className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all ${
        activeTab === id 
          ? 'bg-blue-600 text-white shadow-lg shadow-blue-900/50' 
          : 'text-slate-400 hover:bg-slate-800 hover:text-slate-100'
      }`}
    >
      <Icon size={18} /> {label}
    </button>
  );

  return (
    <div className="flex h-screen bg-slate-950 text-slate-200 font-sans selection:bg-blue-500/30">
      {/* Sidebar */}
      <aside className="w-64 bg-slate-900 border-r border-slate-800 flex flex-col">
        <div className="p-6 border-b border-slate-800">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-emerald-500 rounded-lg flex items-center justify-center text-white font-bold shadow-lg shadow-blue-500/20">
              D
            </div>
            <h1 className="font-bold text-lg tracking-tight">DataProcess</h1>
          </div>
        </div>
        
        <nav className="flex-1 p-4 space-y-1">
          <NavButton id="datasources" icon={Database} label="数据源" />
          <NavButton id="tasks" icon={LayoutDashboard} label="任务" />
          <NavButton id="datamgmt" icon={Folder} label="数据管理" />
          <NavButton id="audit" icon={ShieldAlert} label="审计" />
        </nav>

        <div className="p-4 border-t border-slate-800">
          <div className="flex items-center gap-3 px-4 py-2 rounded-lg bg-slate-950 border border-slate-800 text-xs text-slate-500">
            <Server size={14} />
            <span>v1.0.0 • 已连接</span>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-auto">
        <header className="h-16 border-b border-slate-800 bg-slate-900/50 backdrop-blur sticky top-0 z-10 flex items-center justify-between px-8">
          <h2 className="font-medium text-slate-400">
            工作区 / <span className="text-slate-100">{getTitle()}</span>
          </h2>
          <div className="flex items-center gap-4">
            <div className="w-8 h-8 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-xs font-bold text-slate-400">
              AD
            </div>
          </div>
        </header>
        
        <div className="p-8 max-w-7xl mx-auto">
          {renderContent()}
        </div>
      </main>
    </div>
  );
}

export default App;
