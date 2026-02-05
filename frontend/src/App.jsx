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
          ? 'bg-blue-50 text-blue-600 font-semibold' 
          : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
      }`}
    >
      <Icon size={18} /> {label}
    </button>
  );

  return (
    <div className="flex h-screen bg-white text-slate-800 font-sans selection:bg-blue-100">
      {/* Sidebar */}
      <aside className="w-64 bg-white border-r border-slate-200 flex flex-col shadow-sm z-20">
        <div className="p-6 border-b border-slate-100">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 bg-slate-800 rounded-lg flex items-center justify-center text-white font-bold shadow-md transform hover:scale-105 transition-transform duration-300">
              <span className="text-lg">D</span>
            </div>
            <div className="flex flex-col">
                <h1 className="font-bold text-base tracking-tight text-slate-800">测试数据</h1>
                <span className="text-xs font-bold tracking-tight text-slate-800">预处理分系统</span>
            </div>
          </div>
        </div>
        
        <nav className="flex-1 p-4 space-y-1">
          <NavButton id="datasources" icon={Database} label="数据引接" />
          <NavButton id="tasks" icon={LayoutDashboard} label="数据同步" />
          <NavButton id="datamgmt" icon={Folder} label="数据管理" />
          <NavButton id="audit" icon={ShieldAlert} label="行为预警" />
        </nav>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-auto bg-slate-50">
        <header className="h-14 bg-slate-800 text-white flex items-center justify-between px-8 shadow-sm">
          <h2 className="font-medium text-sm text-slate-300">
             {getTitle()}
          </h2>
          <div className="flex items-center gap-4">
            <div className="w-8 h-8 rounded-full bg-slate-700 border border-slate-600 flex items-center justify-center text-xs font-bold text-slate-300">
              AD
            </div>
          </div>
        </header>
        
        <div className="p-6 max-w-[1600px] mx-auto">
          {renderContent()}
        </div>
      </main>
    </div>
  );
}

export default App;
