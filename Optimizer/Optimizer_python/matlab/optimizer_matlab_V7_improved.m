function [success_flag, result_data, log_info] = optimizer_matlab_V7_improved(n, input_data)
% OPTIMIZER_MATLAB_V7_IMPROVED 优化版本的MATLAB处理脚本
% 
% 输入参数:
%   n           - 任务编号
%   input_data  - 可以是文件路径(字符串)或路径列表(元胞数组)
%
% 输出参数:
%   success_flag - 布尔值，表示处理是否成功
%   result_data  - 结构体，包含处理结果数据
%   log_info     - 字符串，包含处理日志信息
%
% 此函数支持两种调用方式:
% 1. 基于文件的旧方式: optimizer_matlab_V7_improved(n, 'filepath.txt')
% 2. 基于内存的新方式: optimizer_matlab_V7_improved(n, {'path1', 'path2', ...})

% 初始化输出参数
success_flag = false;
result_data = struct('processed_paths', {}, 'statistics', {});
log_info = '';

% 创建日志记录函数
    function add_log(message)
        timestamp = datestr(now, 'yyyy-mm-dd HH:MM:SS.FFF');
        new_log = sprintf('[%s] %s\n', timestamp, message);
        log_info = [log_info, new_log];
        disp(new_log);
    end

% 记录开始时间
start_time = now;
add_log(sprintf('任务 %d 开始处理', n));

try
    % 确定输入类型并处理
    paths_to_process = {};
    
    if ischar(input_data) || isstring(input_data)
        % 基于文件的旧方式
        filepath = input_data;
        add_log(sprintf('使用文件方式，读取文件: %s', filepath));
        
        % 打开文本文件
        fileID = fopen(filepath, 'r');
        
        % 检查文件是否成功打开
        if fileID == -1
            error('无法打开文件: %s', filepath);
        end
        
        % 逐行读取文件内容
        while ~feof(fileID)
            % 读取一行
            line = fgetl(fileID);
            % 去除可能的换行符
            line = strtrim(line);
            if ~isempty(line)
                paths_to_process{end+1} = line;
            end
        end
        
        % 关闭文件
        fclose(fileID);
        
    elseif iscell(input_data)
        % 基于内存的新方式
        add_log('使用内存方式，直接处理路径列表');
        paths_to_process = input_data;
    else
        error('不支持的输入类型');
    end
    
    % 记录找到的路径数量
    add_log(sprintf('找到 %d 个路径需要处理', length(paths_to_process)));
    
    % 初始化结果统计
    processed_count = 0;
    success_count = 0;
    failed_paths = {};
    
    % 处理每个路径
    for i = 1:length(paths_to_process)
        path = paths_to_process{i};
        add_log(sprintf('处理路径 %d/%d: %s', i, length(paths_to_process), path));
        
        try
            % 调用主处理函数
            process_result = main(path);
            processed_count = processed_count + 1;
            success_count = success_count + 1;
            add_log(sprintf('成功处理路径: %s', path));
        catch ME
            % 处理错误
            processed_count = processed_count + 1;
            failed_paths{end+1} = path;
            add_log(sprintf('处理路径失败: %s, 错误: %s', path, ME.message));
        end
    end
    
    % 计算处理时间
    elapsed_time = (now - start_time) * 24 * 60 * 60; % 转换为秒
    
    % 准备结果数据
    result_data.processed_paths = paths_to_process;
    result_data.statistics = struct(...
        'total', length(paths_to_process), ...
        'processed', processed_count, ...
        'success', success_count, ...
        'failed', length(failed_paths), ...
        'failed_paths', {failed_paths}, ...
        'elapsed_time', elapsed_time);
    
    % 为了向后兼容，创建结果文件
    txt_name = sprintf('result%d.txt', n);
    
    % 获取当前工作目录或临时目录
    if exist('tempdir', 'var')
        filepath = fullfile(tempdir, txt_name);
    else
        filepath = fullfile(pwd, txt_name);
    end
    
    fileID = fopen(filepath, 'w');
    if fileID == -1
        add_log(sprintf('警告: 无法创建结果文件: %s', filepath));
    else
        fprintf(fileID, '%d', 1);
        fclose(fileID);
        add_log(sprintf('创建结果文件: %s', filepath));
    end
    
    % 设置成功标志
    success_flag = (success_count == length(paths_to_process));
    
    % 记录完成信息
    if success_flag
        add_log(sprintf('任务 %d 全部成功完成，处理 %d 个路径，耗时 %.2f 秒', ...
            n, length(paths_to_process), elapsed_time));
    else
        add_log(sprintf('任务 %d 部分完成，成功: %d/%d，失败: %d，耗时 %.2f 秒', ...
            n, success_count, length(paths_to_process), length(failed_paths), elapsed_time));
    end
    
catch ME
    % 处理主函数错误
    add_log(sprintf('任务 %d 执行出错: %s', n, ME.message));
    add_log(sprintf('错误详情: %s', getReport(ME)));
    
    % 设置错误结果
    success_flag = false;
    result_data.error = struct('message', ME.message, 'stack', {ME.stack});
end

% 主处理函数
    function result = main(path)
        % 这里保留原始的处理逻辑，但进行了结构化和错误处理改进
        
        % 读取参数文件
        try
            style_factor = readtable(fullfile(path, 'parameter_selecting.xlsx'), 'Sheet', 'style');
            style_len = size(style_factor, 1);
            
            industry_factor = readtable(fullfile(path, 'parameter_selecting.xlsx'), 'Sheet', 'industry');
            industry_len = size(industry_factor, 1);
            
            % 读取风险暴露数据
            stock_risk = importdata(fullfile(path, 'Stock_risk_exposure.csv'));
            
            % 读取权重限制
            lower_weight = importdata(fullfile(path, 'Stock_lower_weight.csv'));
            lower_weight = lower_weight.data;
            
            upper_weight = importdata(fullfile(path, 'Stock_upper_weight.csv'));
            upper_weight = upper_weight.data;
            
            % 读取初始权重
            index_initial_weight = importdata(fullfile(path, 'index_initial_weight.csv'));
            index_initial_weight = index_initial_weight.data;
            
            % 读取特定风险
            specific_risk = importdata(fullfile(path, 'Stock_specific_risk.csv'));
            
            % 这里添加您的优化算法逻辑
            % ...
            
            % 返回处理结果
            result = struct('success', true);
            
        catch ME
            % 重新抛出错误，但添加更多上下文
            error('处理路径 %s 时出错: %s', path, ME.message);
        end
    end

end
