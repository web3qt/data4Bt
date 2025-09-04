#!/bin/bash

# =============================================================================
# 简化服务器环境配置脚本
# 专门用于已有Go环境，只需要安装Docker和ClickHouse的服务器
# =============================================================================

set -euo pipefail

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# =============================================================================
# 主要功能
# =============================================================================

# 检查现有环境
check_existing_environment() {
    log_info "检查现有环境..."
    
    # 检查Go
    if command -v go >/dev/null 2>&1; then
        local go_version=$(go version | awk '{print $3}' | sed 's/go//')
        log_success "Go已安装: $go_version"
    else
        log_error "Go未安装，请先安装Go 1.21+"
        return 1
    fi
    
    # 检查其他基础命令
    local missing_commands=()
    for cmd in curl wget git; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing_commands+=("$cmd")
        fi
    done
    
    if [ ${#missing_commands[@]} -gt 0 ]; then
        log_warn "缺少基础命令: ${missing_commands[*]}"
        install_basic_tools "${missing_commands[@]}"
    else
        log_success "基础命令检查通过"
    fi
}

# 安装基础工具
install_basic_tools() {
    local missing_tools=("$@")
    log_info "安装缺少的基础工具: ${missing_tools[*]}"
    
    if command -v apt-get >/dev/null 2>&1; then
        # Ubuntu/Debian
        sudo apt-get update
        for tool in "${missing_tools[@]}"; do
            sudo apt-get install -y "$tool"
        done
    elif command -v yum >/dev/null 2>&1; then
        # CentOS/RHEL
        for tool in "${missing_tools[@]}"; do
            sudo yum install -y "$tool"
        done
    elif command -v dnf >/dev/null 2>&1; then
        # Fedora
        for tool in "${missing_tools[@]}"; do
            sudo dnf install -y "$tool"
        done
    else
        log_warn "无法识别包管理器，请手动安装: ${missing_tools[*]}"
    fi
}

# 安装Docker
install_docker() {
    log_info "检查并安装Docker..."
    
    # 检查Docker是否已安装
    if command -v docker >/dev/null 2>&1; then
        if docker info >/dev/null 2>&1; then
            log_success "Docker已安装并运行"
            return 0
        else
            log_warn "Docker已安装但未运行，尝试启动..."
            start_docker_service
            return $?
        fi
    fi
    
    log_info "开始安装Docker..."
    
    # 检测操作系统并安装Docker
    if [[ "$OSTYPE" == "darwin"* ]]; then
        log_info "检测到macOS，请手动安装Docker Desktop"
        echo "下载地址: https://www.docker.com/products/docker-desktop/"
        echo "或使用Homebrew: brew install --cask docker"
        return 1
    elif command -v apt-get >/dev/null 2>&1; then
        install_docker_ubuntu
    elif command -v yum >/dev/null 2>&1; then
        install_docker_centos
    elif command -v dnf >/dev/null 2>&1; then
        install_docker_fedora
    else
        install_docker_generic
    fi
}

# Ubuntu/Debian安装Docker
install_docker_ubuntu() {
    log_info "在Ubuntu/Debian上安装Docker..."
    
    # 更新包索引
    sudo apt-get update
    
    # 安装必要的包
    sudo apt-get install -y \
        ca-certificates \
        curl \
        gnupg \
        lsb-release
    
    # 添加Docker官方GPG密钥
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    
    # 添加Docker仓库
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # 安装Docker Engine
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    
    # 启动Docker服务
    sudo systemctl start docker
    sudo systemctl enable docker
    
    # 添加当前用户到docker组
    sudo usermod -aG docker "$USER"
    
    log_success "Docker安装完成"
    log_info "请重新登录或运行 'newgrp docker' 以使用Docker"
}

# CentOS/RHEL安装Docker
install_docker_centos() {
    log_info "在CentOS/RHEL上安装Docker..."
    
    # 安装依赖
    sudo yum install -y yum-utils
    
    # 添加Docker仓库
    sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
    
    # 安装Docker
    sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    
    # 启动Docker服务
    sudo systemctl start docker
    sudo systemctl enable docker
    
    # 添加当前用户到docker组
    sudo usermod -aG docker "$USER"
    
    log_success "Docker安装完成"
}

# Fedora安装Docker
install_docker_fedora() {
    log_info "在Fedora上安装Docker..."
    
    # 安装依赖
    sudo dnf -y install dnf-plugins-core
    
    # 添加Docker仓库
    sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
    
    # 安装Docker
    sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    
    # 启动Docker服务
    sudo systemctl start docker
    sudo systemctl enable docker
    
    # 添加当前用户到docker组
    sudo usermod -aG docker "$USER"
    
    log_success "Docker安装完成"
}

# 通用Docker安装
install_docker_generic() {
    log_info "尝试使用官方脚本安装Docker..."
    
    if curl -fsSL https://get.docker.com | sh; then
        # 启动Docker服务
        if command -v systemctl >/dev/null 2>&1; then
            sudo systemctl start docker
            sudo systemctl enable docker
        fi
        
        # 添加当前用户到docker组
        sudo usermod -aG docker "$USER"
        
        log_success "Docker安装完成"
        log_info "请重新登录或运行 'newgrp docker' 以使用Docker"
    else
        log_error "Docker安装失败，请手动安装"
        return 1
    fi
}

# 启动Docker服务
start_docker_service() {
    log_info "启动Docker服务..."
    
    if command -v systemctl >/dev/null 2>&1; then
        sudo systemctl start docker
        if docker info >/dev/null 2>&1; then
            log_success "Docker服务启动成功"
            return 0
        fi
    elif command -v service >/dev/null 2>&1; then
        sudo service docker start
        if docker info >/dev/null 2>&1; then
            log_success "Docker服务启动成功"
            return 0
        fi
    fi
    
    log_error "Docker服务启动失败"
    return 1
}

# 测试Docker环境
test_docker() {
    log_info "测试Docker环境..."
    
    if docker run --rm hello-world >/dev/null 2>&1; then
        log_success "Docker测试通过"
        return 0
    else
        log_error "Docker测试失败"
        return 1
    fi
}

# 启动ClickHouse
setup_clickhouse() {
    log_info "启动ClickHouse容器..."
    
    # 检查是否已有容器运行
    if docker ps | grep -q "data4bt-clickhouse"; then
        log_success "ClickHouse容器已在运行"
        return 0
    fi
    
    # 检查是否有停止的容器
    if docker ps -a | grep -q "data4bt-clickhouse"; then
        log_info "启动已存在的ClickHouse容器..."
        docker start data4bt-clickhouse
    else
        # 启动新的ClickHouse容器
        log_info "创建新的ClickHouse容器..."
        
        # 先尝试使用docker-compose
        if [ -f "docker-compose.yml" ]; then
            log_info "使用docker-compose启动..."
            docker-compose up -d clickhouse 2>/dev/null || docker compose up -d clickhouse
        else
            log_info "直接创建ClickHouse容器..."
            docker run -d \
                --name data4bt-clickhouse \
                --restart unless-stopped \
                -p 8123:8123 \
                -p 9000:9000 \
                -e CLICKHOUSE_DB=data4BT \
                -e CLICKHOUSE_USER=default \
                -e CLICKHOUSE_PASSWORD=123456 \
                clickhouse/clickhouse-server:23.8-alpine
        fi
    fi
    
    # 等待ClickHouse启动
    log_info "等待ClickHouse启动..."
    local max_wait=60
    local wait_count=0
    
    while [ $wait_count -lt $max_wait ]; do
        if curl -s http://localhost:8123/ping >/dev/null 2>&1; then
            log_success "ClickHouse启动成功"
            return 0
        fi
        sleep 2
        wait_count=$((wait_count + 2))
    done
    
    log_error "ClickHouse启动超时"
    docker logs data4bt-clickhouse 2>/dev/null || true
    return 1
}

# 初始化数据库
init_database() {
    log_info "初始化数据库..."
    
    if [ ! -f "config.yml" ]; then
        if [ -f "configs/config.yml" ]; then
            cp configs/config.yml config.yml
            log_info "复制默认配置文件"
        else
            log_error "找不到配置文件"
            return 1
        fi
    fi
    
    # 初始化数据库表
    if go run cmd/main.go -cmd=init-db; then
        log_success "数据库初始化完成"
    else
        log_error "数据库初始化失败"
        return 1
    fi
}

# 测试系统
test_system() {
    log_info "测试系统功能..."
    
    # 测试发现功能
    if timeout 30 go run cmd/main.go -cmd=discover -symbols=BTCUSDT 2>/dev/null; then
        log_success "系统功能测试通过"
    else
        log_warn "系统功能测试超时（可能是网络问题）"
    fi
}

# 显示完成信息
show_completion() {
    echo ""
    log_success "=== 服务器环境配置完成 ==="
    echo ""
    echo "🎉 现在你可以使用以下命令启动应用："
    echo ""
    echo "   ./start.sh                # 前台运行"
    echo "   ./start.sh --background   # 后台运行"
    echo "   ./start.sh --test         # 测试模式"
    echo ""
    echo "📊 监控地址:"
    echo "   http://$(hostname -I | awk '{print $1}' || echo 'localhost'):8890"
    echo ""
    echo "🗄️ ClickHouse连接信息:"
    echo "   HTTP端口: 8123"
    echo "   Native端口: 9000"
    echo "   用户名: default"
    echo "   密码: 123456"
    echo "   数据库: data4BT"
    echo ""
    echo "📝 管理命令:"
    echo "   ./stop.sh                 # 停止应用"
    echo "   docker logs data4bt-clickhouse  # 查看ClickHouse日志"
    echo ""
}

# 主函数
main() {
    echo "=== Binance Data Loader 简化服务器配置 ==="
    echo ""
    
    # 检查现有环境
    if ! check_existing_environment; then
        log_error "环境检查失败"
        exit 1
    fi
    
    # 安装Docker
    if ! install_docker; then
        log_error "Docker安装失败，请手动安装Docker后再次运行"
        exit 1
    fi
    
    # 如果Docker刚安装，用户可能需要重新登录
    if ! docker info >/dev/null 2>&1; then
        log_info "Docker需要重新登录才能使用，尝试使用sudo运行测试..."
        if sudo docker info >/dev/null 2>&1; then
            log_warn "请重新登录或运行 'newgrp docker'，然后再次运行此脚本"
            exit 0
        fi
    fi
    
    # 测试Docker
    if ! test_docker; then
        log_error "Docker测试失败"
        exit 1
    fi
    
    # 启动ClickHouse
    if ! setup_clickhouse; then
        log_error "ClickHouse设置失败"
        exit 1
    fi
    
    # 初始化数据库
    if ! init_database; then
        log_error "数据库初始化失败"
        exit 1
    fi
    
    # 测试系统
    test_system
    
    # 显示完成信息
    show_completion
}

# 运行主函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi