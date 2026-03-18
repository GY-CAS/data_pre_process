import requests
import json

BASE_URL = 'http://localhost:8000'

print('=== 集成测试：数据源删除前置校验 ===')
print()

# 1. 创建测试数据源
print('1. 创建测试数据源...')
ds_response = requests.post(f'{BASE_URL}/datasources/', json={
    'name': 'test_ds_for_deletion_check',
    'type': 'mysql',
    'description': 'Test datasource for deletion check',
    'connection_info': json.dumps({'host': 'localhost', 'port': 3306})
})
print(f'   创建数据源: {ds_response.status_code}')
ds_list = requests.get(f'{BASE_URL}/datasources/').json()
ds_id = None
for ds in ds_list['data']:
    if ds['name'] == 'test_ds_for_deletion_check':
        ds_id = ds['id']
        break
print(f'   数据源ID: {ds_id}')
print()

# 2. 创建关联任务
print('2. 创建关联任务...')
task_response = requests.post(f'{BASE_URL}/tasks/', json={
    'name': 'test_task_for_ds_deletion',
    'task_type': 'sync',
    'config': json.dumps({'source_id': ds_id, 'target': {'table': 'test_table'}})
})
print(f'   创建任务: {task_response.status_code}')
print()

# 3. 测试获取关联任务
print('3. 测试获取关联任务接口...')
related_response = requests.get(f'{BASE_URL}/datasources/{ds_id}/related-tasks')
print(f'   状态码: {related_response.status_code}')
related_data = related_response.json()
print(f'   关联任务数量: {related_data["related_tasks_count"]}')
print(f'   关联任务列表: {json.dumps(related_data["related_tasks"], indent=2, ensure_ascii=False)}')
print()

# 4. 测试删除被阻止
print('4. 测试删除数据源（应被阻止）...')
delete_response = requests.delete(f'{BASE_URL}/datasources/{ds_id}')
print(f'   状态码: {delete_response.status_code}')
if delete_response.status_code == 409:
    error_detail = delete_response.json()['detail']
    print(f'   错误码: {error_detail["error_code"]}')
    print(f'   错误消息: {error_detail["message"]}')
    print(f'   关联任务数: {error_detail["related_tasks_count"]}')
    print('   ✓ 删除被正确阻止')
else:
    print('   ✗ 删除未被阻止')
print()

# 5. 删除关联任务后再删除数据源
print('5. 删除关联任务后再删除数据源...')
task_list = requests.get(f'{BASE_URL}/tasks/').json()
for task in task_list['items']:
    if task['name'] == 'test_task_for_ds_deletion':
        task_del_response = requests.delete(f'{BASE_URL}/tasks/{task["id"]}')
        print(f'   删除任务: {task_del_response.status_code}')

delete_response2 = requests.delete(f'{BASE_URL}/datasources/{ds_id}')
print(f'   删除数据源: {delete_response2.status_code}')
if delete_response2.status_code == 200:
    print('   ✓ 数据源删除成功')
print()

print('=== 集成测试完成 ===')
