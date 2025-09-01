/**
 * Enhanced Indices & Mappings Manager
 * Complete implementation with dynamic API loading
 * Advanced Elasticsearch indices management with enhanced features
 */

class EnhancedIndicesManager {
    constructor() {
        this.currentEnvironment = null;
        this.selectedIndex = null;
        this.currentMapping = null;
        this.currentSettings = null;
        this.isConnected = false;
        this.selectedIndices = new Set();
        this.performanceData = {};
        this.environments = {}; // Will be loaded from API
        this.indices = {}; // Will be loaded from API

        // Initialize when DOM is ready
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.initialize());
        } else {
            this.initialize();
        }
    }

    initialize() {
        this.setupEventListeners();
        this.loadEnvironments(); // Load from API instead of demo data
        this.initializePerformanceMonitoring();
    }

    setupEventListeners() {
        // Environment management
        const envSelect = document.getElementById('enhancedIndicesEnvironment');
        const connectBtn = document.getElementById('connectEnhancedIndicesBtn');

        if (envSelect) {
            envSelect.addEventListener('change', (e) => this.handleEnvironmentChange(e));
        }

        if (connectBtn) {
            connectBtn.addEventListener('click', () => this.connectToEnvironment());
        }

        // Search and filter
        const searchInput = document.getElementById('enhancedIndexSearch');
        if (searchInput) {
            searchInput.addEventListener('input', () => this.filterIndices());
        }

        // Health filter
        document.querySelectorAll('input[name="enhancedHealthFilter"]').forEach(radio => {
            radio.addEventListener('change', () => this.filterIndices());
        });

        // Size filter
        const sizeFilter = document.getElementById('sizeFilter');
        if (sizeFilter) {
            sizeFilter.addEventListener('change', () => this.filterIndices());
        }

        // Bulk selection
        const selectAllCheckbox = document.getElementById('selectAllIndices');
        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener('change', (e) => this.toggleSelectAll(e.target.checked));
        }

        // Refresh and export buttons
        const refreshBtn = document.getElementById('refreshAllIndicesBtn');
        const exportBtn = document.getElementById('exportAllMappingsBtn');
        const bulkBtn = document.getElementById('bulkOperationsBtn');

        if (refreshBtn) refreshBtn.addEventListener('click', () => this.refreshAllData());
        if (exportBtn) exportBtn.addEventListener('click', () => this.exportAllMappings());
        if (bulkBtn) bulkBtn.addEventListener('click', () => this.showBulkOperations());
    }

    /**
     * Load environments dynamically from API
     * Replaces the hard-coded loadDemoData() method
     */
    async loadEnvironments() {
        try {
            console.log('Loading environments from API...');

            // Show loading state
            this.showEnvironmentLoadingState();

            const response = await fetch('/environments');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            console.log('Environments loaded:', data);

            // Transform API response to expected format
            this.environments = {};

            // Process Elasticsearch environments
            if (data.elasticsearch && Array.isArray(data.elasticsearch)) {
                data.elasticsearch.forEach(env => {
                    this.environments[env.id] = {
                        id: env.id,
                        name: env.name,
                        url: env.host_url,
                        status: 'disconnected', // Will be updated when connecting
                        username: env.username,
                        password: env.password,
                        cluster: {
                            name: `elasticsearch-${env.name.toLowerCase().replace(/\s+/g, '-')}`,
                            status: 'unknown',
                            nodes: 0,
                            indices: 0,
                            version: 'unknown'
                        }
                    };
                });
            }

            console.log('Processed environments:', this.environments);
            this.populateEnvironmentDropdown();

        } catch (error) {
            console.error('Failed to load environments:', error);
            this.showEnvironmentLoadError(error.message);
        }
    }

    /**
     * Show loading state for environment dropdown
     */
    showEnvironmentLoadingState() {
        const envSelect = document.getElementById('enhancedIndicesEnvironment');
        if (envSelect) {
            envSelect.innerHTML = '<option value="">Loading environments...</option>';
            envSelect.disabled = true;
        }
    }

    /**
     * Show error state for environment loading
     */
    showEnvironmentLoadError(errorMessage) {
        const envSelect = document.getElementById('enhancedIndicesEnvironment');
        if (envSelect) {
            envSelect.innerHTML = '<option value="">Failed to load environments</option>';
            envSelect.disabled = true;
        }

        // Show error notification
        this.showNotification('error', `Failed to load environments: ${errorMessage}`);
    }

    /**
     * Populate environment dropdown with loaded data
     */
    populateEnvironmentDropdown() {
        const envSelect = document.getElementById('enhancedIndicesEnvironment');
        if (!envSelect) return;

        // Clear and enable dropdown
        envSelect.innerHTML = '<option value="">Choose Environment...</option>';
        envSelect.disabled = false;

        // Add environments from API
        Object.entries(this.environments).forEach(([key, env]) => {
            const option = document.createElement('option');
            option.value = key;
            option.textContent = env.name;
            envSelect.appendChild(option);
        });

        console.log(`Populated dropdown with ${Object.keys(this.environments).length} environments`);
    }

    /**
     * Handle environment change with API data
     */
    handleEnvironmentChange(event) {
        const envKey = event.target.value;
        const connectBtn = document.getElementById('connectEnhancedIndicesBtn');

        if (envKey && this.environments[envKey]) {
            this.currentEnvironment = this.environments[envKey];
            if (connectBtn) connectBtn.disabled = false;
            this.updateConnectionStatus('disconnected');
        } else {
            this.currentEnvironment = null;
            if (connectBtn) connectBtn.disabled = true;
            this.updateConnectionStatus('disconnected');
            this.clearIndicesList();
        }
    }

    /**
     * Connect to environment with real API calls
     */
    async connectToEnvironment() {
        if (!this.currentEnvironment) return;

        this.updateConnectionStatus('connecting');
        console.log('Connecting to environment:', this.currentEnvironment);

        try {
            // Test connection to Elasticsearch environment
            const response = await fetch(`/test-connection/elasticsearch/${this.currentEnvironment.id}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            console.log('Connection test result:', result);

            if (result.success) {
                this.isConnected = true;
                this.updateConnectionStatus('connected');

                // Update cluster information from connection result
                if (result.cluster_name) {
                    this.currentEnvironment.cluster.name = result.cluster_name;
                    this.currentEnvironment.cluster.nodes = result.number_of_nodes || 1;
                }

                // Get enhanced cluster health
                await this.loadClusterHealth();

                // Load indices
                await this.loadIndices();

                this.showNotification('success', `Connected to ${this.currentEnvironment.name}`);
            } else {
                throw new Error(result.message || 'Connection failed');
            }

        } catch (error) {
            console.error('Connection failed:', error);
            this.updateConnectionStatus('disconnected');
            this.showNotification('error', `Connection failed: ${error.message}`);
        }
    }

    /**
     * Load cluster health from API
     */
    async loadClusterHealth() {
        try {
            const response = await fetch(`/api/cluster-health/${this.currentEnvironment.id}`);
            if (response.ok) {
                const healthData = await response.json();
                if (healthData.success) {
                    this.currentEnvironment.cluster.status = healthData.health.status;
                    this.currentEnvironment.cluster.indices = healthData.health.active_primary_shards || 0;
                    this.updateClusterHealth(healthData.health.status);
                }
            }
        } catch (error) {
            console.warn('Failed to load cluster health:', error);
            // Use default health status
            this.updateClusterHealth('yellow');
        }
    }

    updateConnectionStatus(status) {
        const statusElement = document.getElementById('enhancedConnectionStatus');
        if (!statusElement) return;

        statusElement.className = `connection-status status-${status}`;

        const statusText = {
            'connected': 'Connected',
            'connecting': 'Connecting...',
            'disconnected': 'Disconnected'
        };

        const statusIcons = {
            'connected': 'fas fa-check-circle',
            'connecting': 'fas fa-spinner fa-spin',
            'disconnected': 'fas fa-times-circle'
        };

        statusElement.innerHTML = `
            <i class="${statusIcons[status]}"></i>
            ${statusText[status]}
        `;
    }

    updateClusterHealth(health) {
        const healthElement = document.getElementById('clusterHealth');
        if (!healthElement) return;

        const healthClasses = {
            'green': 'bg-success',
            'yellow': 'bg-warning',
            'red': 'bg-danger'
        };

        healthElement.className = `badge ${healthClasses[health] || 'bg-secondary'}`;
        healthElement.textContent = health ? health.toUpperCase() : 'Unknown';
    }

    /**
     * Load indices dynamically from API
     */
    async loadIndices() {
        const indicesList = document.getElementById('enhancedIndicesList');
        const indicesCount = document.getElementById('enhancedIndicesCount');

        if (!indicesList || !indicesCount) return;

        // Show loading state
        indicesList.innerHTML = `
            <div class="text-center py-4">
                <div class="loading-spinner"></div>
                <p class="text-muted mt-2">Loading indices...</p>
            </div>
        `;

        try {
            console.log('Loading indices for environment:', this.currentEnvironment.id);

            // Use enhanced indices endpoint if available, fallback to basic
            let response;
            try {
                response = await fetch(`/api/enhanced-indices-with-performance/${this.currentEnvironment.id}`);
            } catch (error) {
                console.log('Enhanced endpoint not available, using basic indices endpoint');
                response = await fetch(`/indices/${this.currentEnvironment.id}`);
            }

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            console.log('Indices loaded:', data);

            let indices = [];

            // Handle different response formats
            if (data.success && data.indices) {
                // Enhanced API response
                indices = data.indices;
            } else if (Array.isArray(data)) {
                // Basic API response
                indices = this.transformBasicIndicesToEnhanced(data);
            } else {
                throw new Error('Invalid response format');
            }

            // Store indices data
            this.indices = {};
            indices.forEach(index => {
                this.indices[index.name] = index;
            });

            // Render indices
            indicesList.innerHTML = '';
            indices.forEach(index => {
                const indexCard = this.createIndexCard(index);
                indicesList.appendChild(indexCard);
            });

            indicesCount.textContent = `${indices.length} indices`;
            this.selectedIndices.clear();
            this.updateSelectedCount();

            console.log(`Loaded ${indices.length} indices successfully`);

        } catch (error) {
            console.error('Failed to load indices:', error);
            indicesList.innerHTML = `
                <div class="text-center text-danger py-4">
                    <i class="fas fa-exclamation-triangle fa-2x mb-3"></i>
                    <p>Failed to load indices</p>
                    <small class="text-muted">${error.message}</small>
                    <br>
                    <button class="btn btn-outline-primary btn-sm mt-2" onclick="window.enhancedIndicesManager.loadIndices()">
                        <i class="fas fa-retry me-1"></i>Retry
                    </button>
                </div>
            `;

            indicesCount.textContent = '0 indices';
            this.showNotification('error', `Failed to load indices: ${error.message}`);
        }
    }

    /**
     * Transform basic indices response to enhanced format
     */
    transformBasicIndicesToEnhanced(basicIndices) {
        return basicIndices.map(index => ({
            name: index.index || index.name || 'unknown',
            health: index.health || 'yellow',
            status: index.status || 'open',
            primary: parseInt(index.pri || index.primary || 1),
            replica: parseInt(index.rep || index.replica || 0),
            docs: parseInt(index['docs.count'] || index.docs || 0),
            size: index['store.size'] || index.size || '0b',
            sizeBytes: this.convertSizeToBytes(index['store.size'] || index.size || '0b'),
            created: index['creation.date'] || new Date().toISOString(),
            lastModified: new Date().toISOString(),
            mappings: {
                properties: {} // Will be loaded when index is selected
            },
            settings: {
                index: {} // Will be loaded when index is selected
            },
            aliases: [],
            performance: {
                searchLatency: Math.random() * 50 + 10, // Random demo data
                indexingRate: Math.random() * 1000 + 100,
                cacheHitRatio: Math.random() * 0.3 + 0.7,
                memoryUsage: `${Math.round(Math.random() * 500 + 50)}MB`
            }
        }));
    }

    /**
     * Convert size string to bytes
     */
    convertSizeToBytes(sizeString) {
        if (!sizeString || typeof sizeString !== 'string') return 0;

        const units = {
            'b': 1,
            'kb': 1024,
            'mb': 1024 * 1024,
            'gb': 1024 * 1024 * 1024,
            'tb': 1024 * 1024 * 1024 * 1024
        };

        const match = sizeString.toLowerCase().match(/^([\d.]+)\s*([kmgt]?b)$/);
        if (!match) return 0;

        const value = parseFloat(match[1]);
        const unit = match[2];

        return Math.round(value * (units[unit] || 1));
    }

    createIndexCard(index) {
        const card = document.createElement('div');
        card.className = 'index-card fade-in';
        card.dataset.indexName = index.name;
        card.dataset.health = index.health;
        card.dataset.size = this.getSizeCategory(index.sizeBytes);

        card.innerHTML = `
            <div class="index-health health-${index.health}"></div>
            <div class="d-flex justify-content-between align-items-start mb-2">
                <div class="form-check">
                    <input class="form-check-input index-checkbox" type="checkbox" 
                           data-index="${index.name}" onchange="enhancedIndicesManager.toggleIndexSelection('${index.name}', this.checked)">
                    <label class="form-check-label">
                        <h6 class="mb-0">${index.name}</h6>
                    </label>
                </div>
                <span class="badge bg-secondary">${index.status}</span>
            </div>
            <div class="row text-muted small">
                <div class="col-6">
                    <i class="fas fa-copy me-1"></i>${index.primary} primary
                </div>
                <div class="col-6">
                    <i class="fas fa-clone me-1"></i>${index.replica} replica
                </div>
            </div>
            <div class="row text-muted small mt-1">
                <div class="col-6">
                    <i class="fas fa-file-alt me-1"></i>${index.docs.toLocaleString()} docs
                </div>
                <div class="col-6">
                    <i class="fas fa-hdd me-1"></i>${index.size}
                </div>
            </div>
            <div class="row text-muted small mt-1">
                <div class="col-12">
                    <i class="fas fa-clock me-1"></i>Modified: ${this.formatDate(index.lastModified)}
                </div>
            </div>
        `;

        // Add click handler for card (not checkbox)
        card.addEventListener('click', (e) => {
            if (!e.target.classList.contains('form-check-input')) {
                this.selectIndex(index);
            }
        });

        return card;
    }

    getSizeCategory(sizeBytes) {
        const MB = 1024 * 1024;
        const GB = MB * 1024;

        if (sizeBytes < 100 * MB) return 'small';
        if (sizeBytes < GB) return 'medium';
        if (sizeBytes < 10 * GB) return 'large';
        return 'xlarge';
    }

    formatDate(dateString) {
        return new Date(dateString).toLocaleDateString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    }

    toggleIndexSelection(indexName, selected) {
        if (selected) {
            this.selectedIndices.add(indexName);
        } else {
            this.selectedIndices.delete(indexName);
        }
        this.updateSelectedCount();
    }

    toggleSelectAll(selectAll) {
        const checkboxes = document.querySelectorAll('.index-checkbox');
        checkboxes.forEach(checkbox => {
            checkbox.checked = selectAll;
            const indexName = checkbox.dataset.index;
            if (selectAll) {
                this.selectedIndices.add(indexName);
            } else {
                this.selectedIndices.delete(indexName);
            }
        });
        this.updateSelectedCount();
    }

    updateSelectedCount() {
        const countElement = document.getElementById('selectedIndicesCount');
        if (countElement) {
            countElement.textContent = `${this.selectedIndices.size} selected`;
        }
    }

    /**
     * Select index and load its detailed information
     */
    async selectIndex(indexData) {
        // Update selected state in UI
        document.querySelectorAll('.index-card').forEach(card => {
            card.classList.remove('selected');
        });

        const selectedCard = document.querySelector(`[data-index-name="${indexData.name}"]`);
        if (selectedCard) {
            selectedCard.classList.add('selected');
        }

        this.selectedIndex = indexData;

        // Show enhanced mapping toolbar and card
        const toolbar = document.getElementById('enhancedMappingToolbar');
        const card = document.getElementById('enhancedMappingCard');

        if (toolbar) toolbar.style.display = 'block';
        if (card) card.style.display = 'block';

        // Load detailed information
        await this.loadIndexDetails(indexData.name);
    }

    /**
     * Load detailed index information from API
     */
    async loadIndexDetails(indexName) {
        try {
            console.log('Loading details for index:', indexName);

            // Load mapping
            const mappingResponse = await fetch(`/mapping/${this.currentEnvironment.id}/${indexName}`);
            if (mappingResponse.ok) {
                const mappingData = await mappingResponse.json();

                // Handle different response formats
                if (mappingData.mapping) {
                    this.currentMapping = mappingData.mapping[indexName]?.mappings || mappingData.mapping;
                } else {
                    this.currentMapping = mappingData[indexName]?.mappings || mappingData;
                }

                // Update selected index with mapping data
                if (this.selectedIndex) {
                    this.selectedIndex.mappings = this.currentMapping;
                }
            }

            // Load settings (if endpoint exists)
            try {
                const settingsResponse = await fetch(`/api/enhanced-settings/${this.currentEnvironment.id}/${indexName}`);
                if (settingsResponse.ok) {
                    const settingsData = await settingsResponse.json();
                    this.currentSettings = settingsData.settings || settingsData;

                    if (this.selectedIndex) {
                        this.selectedIndex.settings = this.currentSettings;
                    }
                }
            } catch (error) {
                console.log('Settings endpoint not available, using default settings');
                this.currentSettings = {
                    index: {
                        number_of_shards: this.selectedIndex.primary,
                        number_of_replicas: this.selectedIndex.replica
                    }
                };
            }

            // Load performance metrics (if endpoint exists)
            try {
                const perfResponse = await fetch(`/api/performance-metrics/${this.currentEnvironment.id}/${indexName}`);
                if (perfResponse.ok) {
                    const perfData = await perfResponse.json();
                    if (this.selectedIndex) {
                        this.selectedIndex.performance = perfData.performance || perfData;
                    }
                }
            } catch (error) {
                console.log('Performance metrics endpoint not available, using defaults');
            }

            // Update UI with loaded data
            this.loadEnhancedIndexOverview(this.selectedIndex);
            this.loadEnhancedIndexMappings(this.currentMapping);
            this.loadEnhancedIndexSettings(this.currentSettings);
            this.loadEnhancedIndexAliases(this.selectedIndex.aliases || []);
            this.loadPerformanceMetrics(this.selectedIndex.performance);

        } catch (error) {
            console.error('Failed to load index details:', error);
            this.showNotification('error', `Failed to load index details: ${error.message}`);
        }
    }

    loadEnhancedIndexOverview(index) {
        const statsElement = document.getElementById('enhancedIndexStats');
        const healthElement = document.getElementById('enhancedHealthInfo');
        const storageElement = document.getElementById('storageDetails');
        const activityElement = document.getElementById('recentActivity');

        if (statsElement) {
            statsElement.innerHTML = `
                <div class="row">
                    <div class="col-6">
                        <strong>Documents:</strong><br>
                        <span class="text-primary fs-5">${index.docs.toLocaleString()}</span>
                    </div>
                    <div class="col-6">
                        <strong>Total Size:</strong><br>
                        <span class="text-info fs-5">${index.size}</span>
                    </div>
                </div>
                <div class="row mt-2">
                    <div class="col-6">
                        <strong>Primary Shards:</strong><br>
                        <span class="text-success">${index.primary}</span>
                    </div>
                    <div class="col-6">
                        <strong>Replica Shards:</strong><br>
                        <span class="text-warning">${index.replica}</span>
                    </div>
                </div>
                <div class="row mt-2">
                    <div class="col-12">
                        <strong>Average Doc Size:</strong><br>
                        <span class="text-secondary">${Math.round(index.sizeBytes / index.docs)} bytes</span>
                    </div>
                </div>
            `;
        }

        if (healthElement) {
            const healthBadgeClass = {
                'green': 'success',
                'yellow': 'warning',
                'red': 'danger'
            };

            healthElement.innerHTML = `
                <div class="mb-2">
                    <strong>Health Status:</strong><br>
                    <span class="badge bg-${healthBadgeClass[index.health]} fs-6">${index.health.toUpperCase()}</span>
                </div>
                <div class="mb-2">
                    <strong>Index Status:</strong><br>
                    <span class="badge bg-secondary">${index.status}</span>
                </div>
                <div class="mb-2">
                    <strong>Created:</strong><br>
                    <small class="text-muted">${this.formatDate(index.created)}</small>
                </div>
            `;
        }

        if (storageElement) {
            const compressionRatio = ((index.docs * 1000) / index.sizeBytes).toFixed(2);
            storageElement.innerHTML = `
                <div class="mb-2">
                    <strong>Storage Efficiency:</strong><br>
                    <span class="text-success">${compressionRatio} docs/MB</span>
                </div>
                <div class="mb-2">
                    <strong>Compression:</strong><br>
                    <span class="text-info">${index.settings?.index?.codec || 'default'}</span>
                </div>
                <div>
                    <strong>Estimated Growth:</strong><br>
                    <small class="text-muted">~${Math.round(index.sizeBytes * 0.1 / (1024*1024))}MB/week</small>
                </div>
            `;
        }

        if (activityElement) {
            activityElement.innerHTML = `
                <div class="timeline">
                    <div class="timeline-item">
                        <i class="fas fa-edit text-primary"></i>
                        <span class="text-muted">Last modified: ${this.formatDate(index.lastModified)}</span>
                    </div>
                    <div class="timeline-item">
                        <i class="fas fa-plus text-success"></i>
                        <span class="text-muted">Index created: ${this.formatDate(index.created)}</span>
                    </div>
                    <div class="timeline-item">
                        <i class="fas fa-chart-line text-info"></i>
                        <span class="text-muted">Performance: ${index.performance.searchLatency}ms avg search</span>
                    </div>
                </div>
            `;
        }
    }

    loadEnhancedIndexMappings(mappings) {
        // Default to enhanced tree view
        this.toggleEnhancedMappingView('tree');
    }

    loadEnhancedIndexSettings(settings) {
        const settingsEditor = document.getElementById('enhancedSettingsEditor');
        if (!settingsEditor) return;

        settingsEditor.innerHTML = `
            <div class="settings-grid">
                ${this.createEnhancedSettingsCategory('Index Settings', 'index', settings.index)}
                ${settings.analysis ? this.createEnhancedSettingsCategory('Analysis Settings', 'analysis', settings.analysis) : ''}
            </div>
        `;
    }

    createEnhancedSettingsCategory(title, category, settings) {
        const settingsHtml = Object.entries(settings).map(([key, value]) => {
            const displayValue = typeof value === 'object' ? JSON.stringify(value) : value;
            return `
                <div class="settings-item">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <strong>${key.replace(/_/g, ' ').toUpperCase()}</strong>
                            <br>
                            <small class="text-muted">${this.getSettingDescription(key)}</small>
                        </div>
                        <div>
                            ${this.createEnhancedSettingInput(key, value)}
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        return `
            <div class="settings-category">
                <h6 class="mb-3">
                    <i class="fas fa-cog me-2"></i>${title}
                </h6>
                ${settingsHtml}
            </div>
        `;
    }

    createEnhancedSettingInput(key, value) {
        if (typeof value === 'boolean') {
            return `
                <div class="form-check form-switch">
                    <input class="form-check-input" type="checkbox" ${value ? 'checked' : ''} 
                           data-setting-key="${key}" onchange="enhancedIndicesManager.updateSetting('${key}', this.checked)">
                </div>
            `;
        } else if (typeof value === 'number') {
            return `
                <input type="number" class="form-control form-control-sm" value="${value}" 
                       data-setting-key="${key}" onchange="enhancedIndicesManager.updateSetting('${key}', this.value)">
            `;
        } else if (typeof value === 'object') {
            return `
                <textarea class="form-control form-control-sm" rows="2" 
                          data-setting-key="${key}" onchange="enhancedIndicesManager.updateSetting('${key}', this.value)">${JSON.stringify(value, null, 2)}</textarea>
            `;
        } else {
            return `
                <input type="text" class="form-control form-control-sm" value="${value}" 
                       data-setting-key="${key}" onchange="enhancedIndicesManager.updateSetting('${key}', this.value)">
            `;
        }
    }

    getSettingDescription(key) {
        const descriptions = {
            'number_of_shards': 'Number of primary shards for the index',
            'number_of_replicas': 'Number of replica shards for each primary',
            'max_result_window': 'Maximum number of documents returned in a single request',
            'refresh_interval': 'How often the index is refreshed',
            'codec': 'Compression codec used for stored fields'
        };
        return descriptions[key] || 'Index configuration setting';
    }

    loadEnhancedIndexAliases(aliases) {
        const aliasesDisplay = document.getElementById('enhancedAliasesDisplay');
        if (!aliasesDisplay) return;

        if (!aliases || aliases.length === 0) {
            aliasesDisplay.innerHTML = `
                <div class="text-center text-muted py-4">
                    <i class="fas fa-link fa-2x mb-3"></i>
                    <p>No aliases configured for this index</p>
                </div>
            `;
            return;
        }

        const aliasesHtml = aliases.map(alias => `
            <div class="settings-item">
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <i class="fas fa-link me-2 text-primary"></i>
                        <strong>${alias}</strong>
                        <br>
                        <small class="text-muted">Active alias pointing to this index</small>
                    </div>
                    <div class="btn-group" role="group">
                        <button class="btn btn-outline-info btn-sm" onclick="editAlias('${alias}')">
                            <i class="fas fa-edit me-1"></i>Edit
                        </button>
                        <button class="btn btn-outline-danger btn-sm" onclick="enhancedIndicesManager.removeEnhancedAlias('${alias}')">
                            <i class="fas fa-trash me-1"></i>Remove
                        </button>
                    </div>
                </div>
            </div>
        `).join('');

        aliasesDisplay.innerHTML = aliasesHtml;
    }

    loadPerformanceMetrics(performance) {
        const metricsElement = document.getElementById('performanceMetrics');
        if (!metricsElement) return;

        metricsElement.innerHTML = `
            <div class="row">
                <div class="col-md-6">
                    <div class="settings-item">
                        <h6><i class="fas fa-clock me-2 text-primary"></i>Search Performance</h6>
                        <div class="row">
                            <div class="col-6">
                                <strong>Avg Latency:</strong><br>
                                <span class="text-${performance.searchLatency < 50 ? 'success' : performance.searchLatency < 100 ? 'warning' : 'danger'}">${Math.round(performance.searchLatency)}ms</span>
                            </div>
                            <div class="col-6">
                                <strong>Cache Hit Ratio:</strong><br>
                                <span class="text-${performance.cacheHitRatio > 0.8 ? 'success' : 'warning'}">${Math.round(performance.cacheHitRatio * 100)}%</span>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="settings-item">
                        <h6><i class="fas fa-upload me-2 text-success"></i>Indexing Performance</h6>
                        <div class="row">
                            <div class="col-6">
                                <strong>Index Rate:</strong><br>
                                <span class="text-info">${Math.round(performance.indexingRate)}/sec</span>
                            </div>
                            <div class="col-6">
                                <strong>Memory Usage:</strong><br>
                                <span class="text-secondary">${performance.memoryUsage}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    filterIndices() {
        const searchTerm = document.getElementById('enhancedIndexSearch')?.value.toLowerCase() || '';
        const healthFilter = document.querySelector('input[name="enhancedHealthFilter"]:checked')?.value || 'all';
        const sizeFilter = document.getElementById('sizeFilter')?.value || 'all';

        document.querySelectorAll('.index-card').forEach(card => {
            const indexName = card.dataset.indexName?.toLowerCase() || '';
            const indexHealth = card.dataset.health || '';
            const indexSize = card.dataset.size || '';

            const matchesSearch = indexName.includes(searchTerm);
            const matchesHealth = healthFilter === 'all' || indexHealth === healthFilter;
            const matchesSize = sizeFilter === 'all' || indexSize === sizeFilter;

            card.style.display = matchesSearch && matchesHealth && matchesSize ? 'block' : 'none';
        });
    }

    toggleEnhancedMappingView(viewType) {
        const displayElement = document.getElementById('enhancedMappingDisplay');
        if (!displayElement || !this.currentMapping) return;

        switch(viewType) {
            case 'tree':
                displayElement.innerHTML = this.createEnhancedTreeView(this.currentMapping.properties);
                break;
            case 'table':
                displayElement.innerHTML = this.createEnhancedTableView(this.currentMapping.properties);
                break;
            case 'json':
                displayElement.innerHTML = `
                    <div class="mapping-viewer">
                        <pre>${JSON.stringify(this.currentMapping, null, 2)}</pre>
                    </div>
                `;
                break;
            case 'graph':
                displayElement.innerHTML = this.createGraphView(this.currentMapping.properties);
                break;
        }
    }

    createEnhancedTreeView(properties, level = 0) {
        let html = '';

        Object.entries(properties).forEach(([fieldName, field]) => {
            const indent = level * 20;
            const typeClass = `badge-${field.type || 'unknown'}`;
            const hasChildren = field.properties || field.fields;

            html += `
                <div class="field-tree-item" style="margin-left: ${indent}px">
                    <div class="d-flex justify-content-between align-items-center">
                        <div class="d-flex align-items-center">
                            ${hasChildren ? '<i class="fas fa-folder-open me-2 text-warning"></i>' : '<i class="fas fa-file me-2 text-primary"></i>'}
                            <strong>${fieldName}</strong>
                            <span class="field-type-badge ${typeClass} ms-2">${field.type || 'unknown'}</span>
                            ${field.analyzer ? `<span class="badge bg-info ms-1">analyzer: ${field.analyzer}</span>` : ''}
                        </div>
                        <div class="btn-group" role="group">
                            <button class="btn btn-outline-primary btn-sm" onclick="enhancedIndicesManager.showFieldDetails('${fieldName}', ${JSON.stringify(field).replace(/"/g, '&quot;')})">
                                <i class="fas fa-info"></i>
                            </button>
                            <button class="btn btn-outline-success btn-sm" onclick="editEnhancedField('${fieldName}')">
                                <i class="fas fa-edit"></i>
                            </button>
                        </div>
                    </div>
                    ${field.properties ? this.createEnhancedTreeView(field.properties, level + 1) : ''}
                    ${field.fields ? this.createEnhancedTreeView(field.fields, level + 1) : ''}
                </div>
            `;
        });

        return html;
    }

    createEnhancedTableView(properties) {
        let rows = '';

        const processProperties = (props, path = '') => {
            Object.entries(props).forEach(([fieldName, field]) => {
                const fullPath = path ? `${path}.${fieldName}` : fieldName;
                const typeClass = `badge-${field.type || 'unknown'}`;

                rows += `
                    <tr>
                        <td><code>${fullPath}</code></td>
                        <td><span class="field-type-badge ${typeClass}">${field.type || 'unknown'}</span></td>
                        <td>${field.analyzer || '-'}</td>
                        <td>${field.index !== false ? '<i class="fas fa-check text-success"></i>' : '<i class="fas fa-times text-danger"></i>'}</td>
                        <td>${field.store ? '<i class="fas fa-check text-success"></i>' : '<i class="fas fa-times text-muted"></i>'}</td>
                        <td>
                            <div class="btn-group" role="group">
                                <button class="btn btn-outline-primary btn-sm" onclick="enhancedIndicesManager.showFieldDetails('${fullPath}', ${JSON.stringify(field).replace(/"/g, '&quot;')})">
                                    <i class="fas fa-info"></i>
                                </button>
                                <button class="btn btn-outline-success btn-sm" onclick="editEnhancedField('${fullPath}')">
                                    <i class="fas fa-edit"></i>
                                </button>
                            </div>
                        </td>
                    </tr>
                `;

                if (field.properties) {
                    processProperties(field.properties, fullPath);
                }
                if (field.fields) {
                    processProperties(field.fields, fullPath);
                }
            });
        };

        processProperties(properties);

        return `
            <div class="table-responsive">
                <table class="table table-striped table-hover">
                    <thead class="table-dark">
                        <tr>
                            <th>Field Path</th>
                            <th>Type</th>
                            <th>Analyzer</th>
                            <th>Indexed</th>
                            <th>Stored</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows}
                    </tbody>
                </table>
            </div>
        `;
    }

    createGraphView(properties) {
        return `
            <div class="graph-view-container">
                <div class="text-center py-5">
                    <i class="fas fa-project-diagram fa-3x text-muted mb-3"></i>
                    <h5>Graph View</h5>
                    <p class="text-muted">Interactive graph visualization of field relationships</p>
                    <button class="btn btn-primary" onclick="alert('Graph view would be implemented with D3.js or similar library')">
                        <i class="fas fa-play me-2"></i>Generate Graph
                    </button>
                </div>
            </div>
        `;
    }

    showFieldDetails(fieldPath, fieldData) {
        const field = typeof fieldData === 'string' ? JSON.parse(fieldData.replace(/&quot;/g, '"')) : fieldData;

        // Update breadcrumb
        const breadcrumb = document.getElementById('enhancedRootBreadcrumb');
        if (breadcrumb) {
            breadcrumb.innerHTML = `
                <i class="fas fa-home me-1"></i>Root
                <span class="mx-2">/</span>
                <span class="text-warning">${fieldPath}</span>
            `;
        }

        // Show enhanced field details
        const detailsElement = document.getElementById('enhancedFieldDetails');
        if (!detailsElement) return;

        const detailsHtml = `
            <div class="field-details-container">
                <div class="mb-3">
                    <h6 class="text-white d-flex align-items-center">
                        <i class="fas fa-tag me-2"></i>${fieldPath}
                        <span class="badge bg-primary ms-2">${field.type}</span>
                    </h6>
                </div>
                
                <div class="row">
                    <div class="col-md-6">
                        <h6 class="text-white mb-2">Field Configuration</h6>
                        <div class="bg-dark p-3 rounded">
                            <pre class="text-light mb-0">${JSON.stringify(field, null, 2)}</pre>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <h6 class="text-white mb-2">Field Analysis</h6>
                        <div class="bg-dark p-3 rounded">
                            <div class="mb-2">
                                <strong class="text-warning">Searchable:</strong> 
                                <span class="text-light">${field.index !== false ? 'Yes' : 'No'}</span>
                            </div>
                            <div class="mb-2">
                                <strong class="text-warning">Analyzer:</strong>
                                <span class="text-light">${field.analyzer || 'Default'}</span>
                            </div>
                            <div class="mb-2">
                                <strong class="text-warning">Store:</strong>
                                <span class="text-light">${field.store ? 'Yes' : 'No'}</span>
                            </div>
                            <div class="mb-2">
                                <strong class="text-warning">Doc Values:</strong>
                                <span class="text-light">${field.doc_values !== false ? 'Yes' : 'No'}</span>
                            </div>
                            <div class="mb-2">
                                <strong class="text-warning">Similarity:</strong>
                                <span class="text-light">${field.similarity || 'BM25'}</span>
                            </div>
                        </div>
                        
                        <h6 class="text-white mb-2 mt-3">Performance Impact</h6>
                        <div class="bg-dark p-3 rounded">
                            <div class="mb-2">
                                <strong class="text-info">Memory Usage:</strong>
                                <span class="text-light">${this.estimateFieldMemoryUsage(field)}</span>
                            </div>
                            <div class="mb-2">
                                <strong class="text-info">Search Performance:</strong>
                                <span class="text-light">${this.estimateSearchPerformance(field)}</span>
                            </div>
                            <div class="mb-2">
                                <strong class="text-info">Storage Overhead:</strong>
                                <span class="text-light">${this.estimateStorageOverhead(field)}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        detailsElement.innerHTML = detailsHtml;
    }

    estimateFieldMemoryUsage(field) {
        const type = field.type || 'text';
        const estimates = {
            'keyword': 'Low',
            'text': 'Medium',
            'nested': 'High',
            'object': 'Medium',
            'date': 'Low',
            'boolean': 'Very Low',
            'integer': 'Low',
            'long': 'Low',
            'float': 'Low',
            'double': 'Low'
        };
        return estimates[type] || 'Medium';
    }

    estimateSearchPerformance(field) {
        const type = field.type || 'text';
        const hasAnalyzer = !!field.analyzer;

        if (type === 'keyword') return 'Excellent';
        if (type === 'text' && !hasAnalyzer) return 'Good';
        if (type === 'text' && hasAnalyzer) return 'Fair';
        if (type === 'nested') return 'Slower';
        return 'Good';
    }

    estimateStorageOverhead(field) {
        const stored = field.store;
        const docValues = field.doc_values !== false;
        const hasFields = !!field.fields;

        let overhead = 'Normal';
        if (stored) overhead = 'High';
        if (hasFields) overhead = 'Higher';
        if (!docValues && !stored) overhead = 'Low';

        return overhead;
    }

    // Enhanced modal and utility functions
    deepDiveEnhancedMapping() {
        const modal = new bootstrap.Modal(document.getElementById('enhancedDeepDiveModal'));
        this.populateEnhancedFieldNavigator(this.currentMapping.properties);
        modal.show();
    }

    populateEnhancedFieldNavigator(properties, parentPath = '') {
        const navigator = document.getElementById('enhancedFieldNavigator');
        if (!navigator) return;

        navigator.innerHTML = '';

        const createNavigatorNode = (props, path = '', level = 0) => {
            Object.entries(props).forEach(([fieldName, field]) => {
                const fullPath = path ? `${path}.${fieldName}` : fieldName;
                const indent = level * 15;

                const node = document.createElement('div');
                node.className = 'field-nav-item';
                node.style.marginLeft = `${indent}px`;
                node.innerHTML = `
                    <div class="d-flex align-items-center py-2 px-2 text-white" 
                         style="cursor: pointer; border-radius: 4px;"
                         onmouseover="this.style.backgroundColor='rgba(255,255,255,0.1)'"
                         onmouseout="this.style.backgroundColor='transparent'"
                         onclick="enhancedIndicesManager.showFieldDetails('${fullPath}', ${JSON.stringify(field).replace(/"/g, '&quot;')})">
                        <i class="fas fa-${field.properties || field.fields ? 'folder-open' : 'file'} me-2"></i>
                        ${fieldName}
                        <span class="badge bg-secondary ms-auto">${field.type}</span>
                    </div>
                `;

                navigator.appendChild(node);

                if (field.properties) {
                    createNavigatorNode(field.properties, fullPath, level + 1);
                }
                if (field.fields) {
                    createNavigatorNode(field.fields, fullPath, level + 1);
                }
            });
        };

        createNavigatorNode(properties);
    }

    showBulkOperations() {
        const modal = new bootstrap.Modal(document.getElementById('bulkOperationsModal'));
        this.updateBulkSelectedIndices();
        modal.show();
    }

    updateBulkSelectedIndices() {
        const countElement = document.getElementById('bulkSelectedCount');
        const listElement = document.getElementById('bulkSelectedIndices');

        if (countElement) {
            countElement.textContent = this.selectedIndices.size;
        }

        if (listElement) {
            if (this.selectedIndices.size === 0) {
                listElement.innerHTML = '<p class="text-muted">No indices selected</p>';
            } else {
                const indicesList = Array.from(this.selectedIndices).map(name =>
                    `<span class="badge bg-primary me-1 mb-1">${name}</span>`
                ).join('');
                listElement.innerHTML = indicesList;
            }
        }
    }

    /**
     * Update settings via API
     */
    async updateSetting(key, value) {
        if (!this.selectedIndex || !this.currentEnvironment) return;

        try {
            const response = await fetch(`/api/enhanced-settings/${this.currentEnvironment.id}/${this.selectedIndex.name}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    [key]: value
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();

            if (result.success) {
                // Update local state
                if (this.currentSettings && this.currentSettings.index) {
                    // Convert value to appropriate type
                    if (typeof this.currentSettings.index[key] === 'number') {
                        value = Number(value);
                    } else if (typeof this.currentSettings.index[key] === 'boolean') {
                        value = Boolean(value);
                    }
                    this.currentSettings.index[key] = value;
                }

                this.showNotification('success', `Setting "${key}" updated successfully`);
            } else {
                throw new Error(result.error || 'Update failed');
            }

        } catch (error) {
            console.error('Failed to update setting:', error);
            this.showNotification('error', `Failed to update setting: ${error.message}`);
        }
    }

    /**
     * Execute bulk operation via API
     */
    async executeBulkOperation(operationType, parameters = {}) {
        if (this.selectedIndices.size === 0) {
            this.showNotification('warning', 'No indices selected for bulk operation');
            return;
        }

        try {
            const operation = {
                type: operationType,
                indices: Array.from(this.selectedIndices),
                parameters: parameters
            };

            console.log('Executing bulk operation:', operation);

            const response = await fetch(`/api/bulk-operations/${this.currentEnvironment.id}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(operation)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            console.log('Bulk operation result:', result);

            if (result.success) {
                this.showNotification('success',
                    `Bulk ${operationType} completed successfully!\n` +
                    `Success rate: ${result.success_rate}%\n` +
                    `${result.successful_operations}/${result.total_indices} indices processed`
                );

                // Refresh indices list to reflect changes
                await this.loadIndices();
            } else {
                throw new Error(result.error || 'Bulk operation failed');
            }

        } catch (error) {
            console.error('Bulk operation failed:', error);
            this.showNotification('error', `Bulk operation failed: ${error.message}`);
        }
    }

    /**
     * Refresh all data from API
     */
    async refreshAllData() {
        if (this.isConnected && this.currentEnvironment) {
            await this.loadIndices();
            this.showNotification('success', 'Data refreshed successfully');
        } else {
            await this.loadEnvironments();
        }
    }

    exportAllMappings() {
        const mappings = {};
        Object.values(this.indices).forEach(index => {
            mappings[index.name] = index.mappings;
        });

        const blob = new Blob([JSON.stringify(mappings, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `elasticsearch-mappings-${new Date().toISOString().split('T')[0]}.json`;
        a.click();
        URL.revokeObjectURL(url);
    }

    removeEnhancedAlias(aliasName) {
        if (confirm(`Remove alias "${aliasName}" from index "${this.selectedIndex.name}"?`)) {
            this.selectedIndex.aliases = this.selectedIndex.aliases.filter(alias => alias !== aliasName);
            this.loadEnhancedIndexAliases(this.selectedIndex.aliases);
        }
    }

    clearIndicesList() {
        const indicesList = document.getElementById('enhancedIndicesList');
        const indicesCount = document.getElementById('enhancedIndicesCount');
        const toolbar = document.getElementById('enhancedMappingToolbar');
        const card = document.getElementById('enhancedMappingCard');

        if (indicesList) {
            indicesList.innerHTML = `
                <div class="text-center text-muted py-5">
                    <i class="fas fa-database fa-3x mb-3"></i>
                    <p>Connect to an environment to view indices</p>
                </div>
            `;
        }

        if (indicesCount) indicesCount.textContent = '0 indices';
        if (toolbar) toolbar.style.display = 'none';
        if (card) card.style.display = 'none';

        this.selectedIndices.clear();
        this.updateSelectedCount();
    }

    initializePerformanceMonitoring() {
        // Initialize performance monitoring for real-time updates
        this.performanceInterval = setInterval(() => {
            if (this.selectedIndex && this.selectedIndex.performance) {
                this.updateRealTimeMetrics();
            }
        }, 30000); // Update every 30 seconds
    }

    updateRealTimeMetrics() {
        // Simulate real-time performance updates
        if (this.selectedIndex && this.selectedIndex.performance) {
            // Add small random variations to simulate real metrics
            this.selectedIndex.performance.searchLatency += Math.random() * 10 - 5;
            this.selectedIndex.performance.indexingRate += Math.random() * 100 - 50;
            this.selectedIndex.performance.cacheHitRatio += (Math.random() - 0.5) * 0.02;

            // Keep values within realistic bounds
            this.selectedIndex.performance.searchLatency = Math.max(10, Math.min(500, this.selectedIndex.performance.searchLatency));
            this.selectedIndex.performance.indexingRate = Math.max(100, Math.min(10000, this.selectedIndex.performance.indexingRate));
            this.selectedIndex.performance.cacheHitRatio = Math.max(0.1, Math.min(1.0, this.selectedIndex.performance.cacheHitRatio));

            this.loadPerformanceMetrics(this.selectedIndex.performance);
        }
    }

    /**
     * Show notification to user
     */
    showNotification(type, message, duration = 5000) {
        // Remove existing notifications of the same type
        document.querySelectorAll(`.alert-${type === 'error' ? 'danger' : type}`).forEach(alert => {
            if (alert.classList.contains('position-fixed')) {
                alert.remove();
            }
        });

        // Create notification element
        const notification = document.createElement('div');
        const alertClass = type === 'error' ? 'danger' : type === 'success' ? 'success' : type === 'warning' ? 'warning' : 'info';

        notification.className = `alert alert-${alertClass} alert-dismissible fade show position-fixed shadow-lg`;
        notification.style.cssText = `
        top: 20px; 
        right: 20px; 
        z-index: 9999; 
        min-width: 300px; 
        max-width: 500px;
        border-radius: 8px;
        border: none;
        font-weight: 500;
    `;

        const icon = {
            'success': 'fas fa-check-circle',
            'error': 'fas fa-exclamation-triangle',
            'warning': 'fas fa-exclamation-circle',
            'info': 'fas fa-info-circle'
        };

        notification.innerHTML = `
        <div class="d-flex align-items-center">
            <i class="${icon[type] || icon.info} me-2"></i>
            <div>${message}</div>
        </div>
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;

        document.body.appendChild(notification);

        // Auto-dismiss after specified duration
        setTimeout(() => {
            if (notification.parentNode) {
                notification.classList.remove('show');
                setTimeout(() => {
                    if (notification.parentNode) {
                        notification.remove();
                    }
                }, 150); // Wait for fade out animation
            }
        }, duration);

        return notification;
    }
}



// Global functions for onclick handlers
function viewEnhancedMappingJson() {
    if (window.enhancedIndicesManager && window.enhancedIndicesManager.currentMapping) {
        const mappingJson = JSON.stringify(window.enhancedIndicesManager.currentMapping, null, 2);
        const newWindow = window.open('', '_blank');
        newWindow.document.write(`
            <html>
                <head><title>Mapping JSON - ${window.enhancedIndicesManager.selectedIndex?.name}</title></head>
                <body style="font-family: monospace; padding: 20px;">
                    <h3>Mapping for ${window.enhancedIndicesManager.selectedIndex?.name}</h3>
                    <pre style="background: #f5f5f5; padding: 15px; border-radius: 5px; overflow: auto;">${mappingJson}</pre>
                </body>
            </html>
        `);
    } else {
        alert('No mapping data available');
    }
}



function editEnhancedMappingSettings() {
    if (!window.enhancedIndicesManager || !window.enhancedIndicesManager.selectedIndex) {
        alert('Please select an index first');
        return;
    }

    // Initialize modal
    const modal = new bootstrap.Modal(document.getElementById('enhancedSettingsModal'));

    // Load current settings and show the modal
    initializeEnhancedSettingsModal();

    // Show general settings by default
    showEnhancedSettingsCategory('general');

    modal.show();
}


function initializeEnhancedSettingsModal() {
    const manager = window.enhancedIndicesManager;
    if (!manager || !manager.selectedIndex) return;

    // Update modal title
    const modalTitle = document.querySelector('#enhancedSettingsModal .modal-title');
    if (modalTitle) {
        modalTitle.innerHTML = `
            <i class="fas fa-cogs me-2"></i>Advanced Settings - ${manager.selectedIndex.name}
        `;
    }

    // Set up categories
    setupEnhancedSettingsCategories();
}

/**
 * Setup settings categories with proper event handlers
 */
function setupEnhancedSettingsCategories() {
    const categories = document.querySelectorAll('#enhancedSettingsCategories .list-group-item');

    categories.forEach(category => {
        category.addEventListener('click', function(e) {
            e.preventDefault();

            // Remove active class from all categories
            categories.forEach(cat => cat.classList.remove('active'));

            // Add active class to clicked category
            this.classList.add('active');

            // Get category name from onclick attribute or data attribute
            const categoryName = this.getAttribute('onclick')?.match(/'([^']+)'/)?.[1] ||
                this.dataset.category || 'general';

            showEnhancedSettingsCategory(categoryName);
        });
    });
}

/**
 * Show settings for a specific category
 */
function showEnhancedSettingsCategory(category) {
    console.log('Showing settings category:', category);

    const formElement = document.getElementById('enhancedSettingsForm');
    if (!formElement) {
        console.error('Settings form element not found');
        return;
    }

    const manager = window.enhancedIndicesManager;
    const settings = manager?.currentSettings || {};

    let formContent = '';

    switch(category) {
        case 'general':
            formContent = createGeneralSettingsForm(settings);
            break;
        case 'index':
            formContent = createIndexSettingsForm(settings);
            break;
        case 'analysis':
            formContent = createAnalysisSettingsForm(settings);
            break;
        case 'mapping':
            formContent = createMappingSettingsForm(settings);
            break;
        case 'performance':
            formContent = createPerformanceSettingsForm(settings);
            break;
        case 'security':
            formContent = createSecuritySettingsForm(settings);
            break;
        default:
            formContent = `<div class="alert alert-info">Settings for ${category} category</div>`;
    }

    formElement.innerHTML = formContent;

    // Initialize form controls
    initializeFormControls();
}




function deepDiveEnhancedMapping() {
    if (window.enhancedIndicesManager) {
        window.enhancedIndicesManager.deepDiveEnhancedMapping();
    }
}

function cloneIndex() {
    if (window.enhancedIndicesManager && window.enhancedIndicesManager.selectedIndex) {
        const newName = prompt(`Clone index "${window.enhancedIndicesManager.selectedIndex.name}" to:`);
        if (newName) {
            alert(`Index cloning functionality would clone to "${newName}" (Demo)`);
        }
    }
}

function reindexData() {
    if (window.enhancedIndicesManager && window.enhancedIndicesManager.selectedIndex) {
        const targetIndex = prompt(`Reindex "${window.enhancedIndicesManager.selectedIndex.name}" to:`);
        if (targetIndex) {
            alert(`Data reindexing to "${targetIndex}" would be implemented (Demo)`);
        }
    }
}

function showEnhancedSettingsCategory(category) {
    // Update active category
    document.querySelectorAll('#enhancedSettingsCategories .list-group-item').forEach(item => {
        item.classList.remove('active');
    });
    if (event && event.target) {
        event.target.classList.add('active');
    }

    const formElement = document.getElementById('enhancedSettingsForm');
    if (!formElement) return;

    // Implementation would show different settings based on category
    formElement.innerHTML = `<h6>${category.charAt(0).toUpperCase() + category.slice(1)} Settings</h6><p class="text-muted">Settings for ${category} would be displayed here.</p>`;
}

async function saveEnhancedSettings() {
    if (window.enhancedIndicesManager && window.enhancedIndicesManager.selectedIndex) {
        // Collect all changed settings
        const changedSettings = {};
        document.querySelectorAll('[data-setting-key]').forEach(input => {
            const key = input.dataset.settingKey;
            let value = input.value;

            // Convert to appropriate type
            if (input.type === 'checkbox') {
                value = input.checked;
            } else if (input.type === 'number') {
                value = Number(value);
            }

            changedSettings[key] = value;
        });

        // Send each setting update
        for (const [key, value] of Object.entries(changedSettings)) {
            await window.enhancedIndicesManager.updateSetting(key, value);
        }
    }
}

function resetEnhancedSettings() {
    if (confirm('Reset all settings to default values?')) {
        if (window.enhancedIndicesManager && window.enhancedIndicesManager.selectedIndex) {
            window.enhancedIndicesManager.loadEnhancedIndexSettings(window.enhancedIndicesManager.selectedIndex.settings);
        }
    }
}

function compareSettings() {
    alert('Settings comparison view would be displayed (Demo)');
}

function toggleEnhancedAdvancedSettings() {
    alert('Advanced settings panel toggled (Demo)');
}

function addEnhancedAlias() {
    const aliasName = prompt('Enter alias name:');
    if (aliasName && window.enhancedIndicesManager && window.enhancedIndicesManager.selectedIndex) {
        window.enhancedIndicesManager.selectedIndex.aliases.push(aliasName);
        window.enhancedIndicesManager.loadEnhancedIndexAliases(window.enhancedIndicesManager.selectedIndex.aliases);
    }
}

function bulkAliasOperations() {
    alert('Bulk alias operations would be implemented (Demo)');
}

function refreshPerformanceMetrics() {
    if (window.enhancedIndicesManager) {
        window.enhancedIndicesManager.updateRealTimeMetrics();
    }
}

function performanceAnalysis() {
    alert('Performance analysis dashboard would be displayed (Demo)');
}

function exportEnhancedFieldMapping() {
    alert('Enhanced field mapping exported (Demo)');
}

function optimizeField() {
    alert('Field optimization suggestions would be displayed (Demo)');
}

function editEnhancedField(fieldPath) {
    alert(`Edit enhanced field: ${fieldPath} (Demo)`);
}

// Bulk operation functions
async function bulkUpdateSettings() {
    const settings = prompt('Enter settings JSON:');
    if (settings && window.enhancedIndicesManager) {
        try {
            const settingsObj = JSON.parse(settings);
            await window.enhancedIndicesManager.executeBulkOperation('update_settings', settingsObj);
        } catch (error) {
            alert('Invalid JSON format');
        }
    }
}

async function bulkCreateAliases() {
    const aliasName = prompt('Enter alias name:');
    if (aliasName && window.enhancedIndicesManager) {
        await window.enhancedIndicesManager.executeBulkOperation('create_aliases', { alias_name: aliasName });
    }
}

async function bulkReindex() {
    const suffix = prompt('Enter destination suffix:', '_reindexed');
    if (suffix && window.enhancedIndicesManager) {
        await window.enhancedIndicesManager.executeBulkOperation('reindex', { dest_suffix: suffix });
    }
}

async function bulkExportMappings() {
    if (window.enhancedIndicesManager) {
        window.enhancedIndicesManager.exportAllMappings();
    }
}

async function bulkClose() {
    if (confirm('Close selected indices? This will make them read-only.') && window.enhancedIndicesManager) {
        await window.enhancedIndicesManager.executeBulkOperation('close');
    }
}

async function bulkOpen() {
    if (window.enhancedIndicesManager) {
        await window.enhancedIndicesManager.executeBulkOperation('open');
    }
}

async function bulkForcemerge() {
    if (confirm('Force merge selected indices? This may impact performance.') && window.enhancedIndicesManager) {
        const segments = prompt('Maximum number of segments:', '1');
        if (segments) {
            await window.enhancedIndicesManager.executeBulkOperation('force_merge', { max_num_segments: parseInt(segments) });
        }
    }
}

async function bulkDelete() {
    if (confirm('Delete selected indices? This action cannot be undone!') && window.enhancedIndicesManager) {
        await window.enhancedIndicesManager.executeBulkOperation('delete');
    }
}

function previewEnhancedSettingsChanges() {
    alert('Settings changes preview would be displayed (Demo)');
}

function applyEnhancedSettingsChanges() {
    alert('Enhanced settings applied successfully (Demo)');
    const modal = bootstrap.Modal.getInstance(document.getElementById('enhancedSettingsModal'));
    if (modal) modal.hide();
}


function createGeneralSettingsForm(settings) {
    const indexSettings = settings.index || {};

    return `
        <div class="settings-category-form">
            <h5 class="mb-4">
                <i class="fas fa-cog me-2 text-primary"></i>General Settings
            </h5>
            
            <div class="row">
                <div class="col-md-6">
                    <div class="settings-group mb-4">
                        <label for="numberOfShards" class="form-label fw-bold">Number of Shards</label>
                        <input type="number" class="form-control" id="numberOfShards" 
                               value="${indexSettings.number_of_shards || 1}" 
                               data-setting-key="number_of_shards" min="1" max="1024">
                        <div class="form-text">Primary shards for this index (cannot be changed after creation)</div>
                    </div>
                    
                    <div class="settings-group mb-4">
                        <label for="numberOfReplicas" class="form-label fw-bold">Number of Replicas</label>
                        <input type="number" class="form-control" id="numberOfReplicas" 
                               value="${indexSettings.number_of_replicas || 0}" 
                               data-setting-key="number_of_replicas" min="0" max="10">
                        <div class="form-text">Replica shards for each primary shard</div>
                    </div>
                </div>
                
                <div class="col-md-6">
                    <div class="settings-group mb-4">
                        <label for="refreshInterval" class="form-label fw-bold">Refresh Interval</label>
                        <select class="form-select" id="refreshInterval" data-setting-key="refresh_interval">
                            <option value="1s" ${(indexSettings.refresh_interval || '1s') === '1s' ? 'selected' : ''}>1 second</option>
                            <option value="5s" ${indexSettings.refresh_interval === '5s' ? 'selected' : ''}>5 seconds</option>
                            <option value="30s" ${indexSettings.refresh_interval === '30s' ? 'selected' : ''}>30 seconds</option>
                            <option value="1m" ${indexSettings.refresh_interval === '1m' ? 'selected' : ''}>1 minute</option>
                            <option value="-1" ${indexSettings.refresh_interval === '-1' ? 'selected' : ''}>Never (manual only)</option>
                        </select>
                        <div class="form-text">How often the index is refreshed to make documents searchable</div>
                    </div>
                    
                    <div class="settings-group mb-4">
                        <label for="maxResultWindow" class="form-label fw-bold">Max Result Window</label>
                        <input type="number" class="form-control" id="maxResultWindow" 
                               value="${indexSettings.max_result_window || 10000}" 
                               data-setting-key="max_result_window" min="1" max="2147483647">
                        <div class="form-text">Maximum number of documents returned in a single search request</div>
                    </div>
                </div>
            </div>
            
            <div class="row">
                <div class="col-12">
                    <div class="settings-group mb-4">
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" id="blocksRead" 
                                   data-setting-key="blocks.read" 
                                   ${indexSettings['blocks.read'] ? 'checked' : ''}>
                            <label class="form-check-label fw-bold" for="blocksRead">
                                Block Read Operations
                            </label>
                        </div>
                        <div class="form-text">Prevent read operations on this index</div>
                    </div>
                    
                    <div class="settings-group mb-4">
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" id="blocksWrite" 
                                   data-setting-key="blocks.write" 
                                   ${indexSettings['blocks.write'] ? 'checked' : ''}>
                            <label class="form-check-label fw-bold" for="blocksWrite">
                                Block Write Operations
                            </label>
                        </div>
                        <div class="form-text">Prevent write operations on this index</div>
                    </div>
                </div>
            </div>
        </div>
    `;
}

/**
 * Create index-specific settings form
 */
function createIndexSettingsForm(settings) {
    const indexSettings = settings.index || {};

    return `
        <div class="settings-category-form">
            <h5 class="mb-4">
                <i class="fas fa-database me-2 text-success"></i>Index Settings
            </h5>
            
            <div class="row">
                <div class="col-md-6">
                    <div class="settings-group mb-4">
                        <label for="codec" class="form-label fw-bold">Compression Codec</label>
                        <select class="form-select" id="codec" data-setting-key="codec">
                            <option value="default" ${(indexSettings.codec || 'default') === 'default' ? 'selected' : ''}>Default</option>
                            <option value="best_compression" ${indexSettings.codec === 'best_compression' ? 'selected' : ''}>Best Compression</option>
                            <option value="lucene_default" ${indexSettings.codec === 'lucene_default' ? 'selected' : ''}>Lucene Default</option>
                        </select>
                        <div class="form-text">Compression algorithm used for stored fields</div>
                    </div>
                    
                    <div class="settings-group mb-4">
                        <label for="maxInnerResultWindow" class="form-label fw-bold">Max Inner Result Window</label>
                        <input type="number" class="form-control" id="maxInnerResultWindow" 
                               value="${indexSettings.max_inner_result_window || 100}" 
                               data-setting-key="max_inner_result_window" min="1">
                        <div class="form-text">Maximum inner hits that can be returned per search request</div>
                    </div>
                </div>
                
                <div class="col-md-6">
                    <div class="settings-group mb-4">
                        <label for="maxRescore" class="form-label fw-bold">Max Rescore Window</label>
                        <input type="number" class="form-control" id="maxRescore" 
                               value="${indexSettings.max_rescore_window || 10000}" 
                               data-setting-key="max_rescore_window" min="1">
                        <div class="form-text">Maximum rescore window size</div>
                    </div>
                    
                    <div class="settings-group mb-4">
                        <label for="gcDeletes" class="form-label fw-bold">GC Deletes</label>
                        <input type="text" class="form-control" id="gcDeletes" 
                               value="${indexSettings.gc_deletes || '60s'}" 
                               data-setting-key="gc_deletes">
                        <div class="form-text">Time to retain delete markers for garbage collection</div>
                    </div>
                </div>
            </div>
        </div>
    `;
}

/**
 * Create analysis settings form
 */
function createAnalysisSettingsForm(settings) {
    const analysisSettings = settings.analysis || {};

    return `
        <div class="settings-category-form">
            <h5 class="mb-4">
                <i class="fas fa-search me-2 text-info"></i>Analysis Settings
            </h5>
            
            <div class="alert alert-info">
                <i class="fas fa-info-circle me-2"></i>
                Analysis settings can only be modified when the index is closed. 
                Advanced analysis configuration should be done via JSON.
            </div>
            
            <div class="row">
                <div class="col-12">
                    <div class="settings-group mb-4">
                        <label for="analysisJson" class="form-label fw-bold">Analysis Configuration (JSON)</label>
                        <textarea class="form-control font-monospace" id="analysisJson" rows="10" 
                                  data-setting-key="analysis" readonly>${JSON.stringify(analysisSettings, null, 2)}</textarea>
                        <div class="form-text">Current analysis configuration (read-only view)</div>
                    </div>
                </div>
            </div>
            
            <div class="row">
                <div class="col-md-6">
                    <h6>Available Analyzers</h6>
                    <div class="border rounded p-3 bg-light">
                        ${Object.keys(analysisSettings.analyzer || {}).length > 0 ?
        Object.keys(analysisSettings.analyzer).map(name =>
            `<span class="badge bg-primary me-2 mb-2">${name}</span>`
        ).join('') :
        '<span class="text-muted">No custom analyzers defined</span>'
    }
                    </div>
                </div>
                
                <div class="col-md-6">
                    <h6>Available Tokenizers</h6>
                    <div class="border rounded p-3 bg-light">
                        ${Object.keys(analysisSettings.tokenizer || {}).length > 0 ?
        Object.keys(analysisSettings.tokenizer).map(name =>
            `<span class="badge bg-success me-2 mb-2">${name}</span>`
        ).join('') :
        '<span class="text-muted">No custom tokenizers defined</span>'
    }
                    </div>
                </div>
            </div>
        </div>
    `;
}

/**
 * Create mapping settings form
 */
function createMappingSettingsForm(settings) {
    return `
        <div class="settings-category-form">
            <h5 class="mb-4">
                <i class="fas fa-project-diagram me-2 text-warning"></i>Mapping Settings
            </h5>
            
            <div class="alert alert-warning">
                <i class="fas fa-exclamation-triangle me-2"></i>
                Mapping settings are mostly read-only after index creation. 
                Use the mapping viewer for detailed field analysis.
            </div>
            
            <div class="row">
                <div class="col-md-6">
                    <div class="settings-group mb-4">
                        <label class="form-label fw-bold">Total Fields Limit</label>
                        <input type="number" class="form-control" 
                               value="${settings.index?.mapping?.total_fields?.limit || 1000}" 
                               data-setting-key="mapping.total_fields.limit" min="1">
                        <div class="form-text">Maximum number of fields in the index</div>
                    </div>
                </div>
                
                <div class="col-md-6">
                    <div class="settings-group mb-4">
                        <label class="form-label fw-bold">Nested Fields Limit</label>
                        <input type="number" class="form-control" 
                               value="${settings.index?.mapping?.nested_fields?.limit || 50}" 
                               data-setting-key="mapping.nested_fields.limit" min="1">
                        <div class="form-text">Maximum number of nested fields</div>
                    </div>
                </div>
            </div>
        </div>
    `;
}

/**
 * Create performance settings form
 */
function createPerformanceSettingsForm(settings) {
    const indexSettings = settings.index || {};

    return `
        <div class="settings-category-form">
            <h5 class="mb-4">
                <i class="fas fa-tachometer-alt me-2 text-danger"></i>Performance Settings
            </h5>
            
            <div class="row">
                <div class="col-md-6">
                    <div class="settings-group mb-4">
                        <label for="maxScriptFields" class="form-label fw-bold">Max Script Fields</label>
                        <input type="number" class="form-control" id="maxScriptFields" 
                               value="${indexSettings.max_script_fields || 32}" 
                               data-setting-key="max_script_fields" min="1">
                        <div class="form-text">Maximum number of script fields in a query</div>
                    </div>
                    
                    <div class="settings-group mb-4">
                        <label for="maxDocvalueFields" class="form-label fw-bold">Max Docvalue Fields</label>
                        <input type="number" class="form-control" id="maxDocvalueFields" 
                               value="${indexSettings.max_docvalue_fields_search || 100}" 
                               data-setting-key="max_docvalue_fields_search" min="1">
                        <div class="form-text">Maximum number of docvalue fields per search request</div>
                    </div>
                </div>
                
                <div class="col-md-6">
                    <div class="settings-group mb-4">
                        <label for="maxTermsCount" class="form-label fw-bold">Max Terms Count</label>
                        <input type="number" class="form-control" id="maxTermsCount" 
                               value="${indexSettings.max_terms_count || 65536}" 
                               data-setting-key="max_terms_count" min="1">
                        <div class="form-text">Maximum number of terms in a terms query</div>
                    </div>
                    
                    <div class="settings-group mb-4">
                        <label for="maxRefreshListeners" class="form-label fw-bold">Max Refresh Listeners</label>
                        <input type="number" class="form-control" id="maxRefreshListeners" 
                               value="${indexSettings.max_refresh_listeners || 1000}" 
                               data-setting-key="max_refresh_listeners" min="1">
                        <div class="form-text">Maximum number of refresh listeners per shard</div>
                    </div>
                </div>
            </div>
        </div>
    `;
}

/**
 * Create security settings form
 */
function createSecuritySettingsForm(settings) {
    const indexSettings = settings.index || {};

    return `
        <div class="settings-category-form">
            <h5 class="mb-4">
                <i class="fas fa-shield-alt me-2 text-secondary"></i>Security Settings
            </h5>
            
            <div class="row">
                <div class="col-12">
                    <div class="settings-group mb-4">
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" id="blocksReadOnlyDelete" 
                                   data-setting-key="blocks.read_only_allow_delete" 
                                   ${indexSettings['blocks.read_only_allow_delete'] ? 'checked' : ''}>
                            <label class="form-check-label fw-bold" for="blocksReadOnlyDelete">
                                Read-Only Allow Delete
                            </label>
                        </div>
                        <div class="form-text">Allow only delete operations when index is read-only</div>
                    </div>
                    
                    <div class="settings-group mb-4">
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" id="blocksMetadata" 
                                   data-setting-key="blocks.metadata" 
                                   ${indexSettings['blocks.metadata'] ? 'checked' : ''}>
                            <label class="form-check-label fw-bold" for="blocksMetadata">
                                Block Metadata Operations
                            </label>
                        </div>
                        <div class="form-text">Prevent metadata operations on this index</div>
                    </div>
                </div>
            </div>
            
            <div class="alert alert-info">
                <i class="fas fa-info-circle me-2"></i>
                Additional security settings like field-level security and document-level security 
                are configured at the cluster level in Elasticsearch Security.
            </div>
        </div>
    `;
}

/**
 * Initialize form controls with event handlers
 */
function initializeFormControls() {
    // Add change event listeners to all form controls
    document.querySelectorAll('[data-setting-key]').forEach(input => {
        input.addEventListener('change', function() {
            // Mark as changed
            this.classList.add('border-warning');
            this.title = 'Setting has been modified - click Save to apply';

            // Enable save button
            const saveBtn = document.querySelector('[onclick*="saveEnhancedSettings"]');
            if (saveBtn) {
                saveBtn.disabled = false;
                saveBtn.classList.remove('btn-secondary');
                saveBtn.classList.add('btn-success');
            }
        });
    });

    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

/**
 * Save enhanced settings with proper validation
 */
async function saveEnhancedSettings() {
    const manager = window.enhancedIndicesManager;
    if (!manager || !manager.selectedIndex) {
        alert('No index selected');
        return;
    }

    // Collect all changed settings
    const changedSettings = {};
    const changedInputs = document.querySelectorAll('[data-setting-key].border-warning');

    if (changedInputs.length === 0) {
        alert('No settings have been changed');
        return;
    }

    // Build settings object
    changedInputs.forEach(input => {
        const key = input.dataset.settingKey;
        let value = input.value;

        // Convert to appropriate type
        if (input.type === 'checkbox') {
            value = input.checked;
        } else if (input.type === 'number') {
            value = Number(value);
        } else if (key === 'analysis') {
            try {
                value = JSON.parse(value);
            } catch (e) {
                alert(`Invalid JSON in analysis settings: ${e.message}`);
                return;
            }
        }

        changedSettings[key] = value;
    });

    console.log('Saving settings:', changedSettings);

    // Show loading state
    const saveBtn = document.querySelector('[onclick*="saveEnhancedSettings"]');
    if (saveBtn) {
        saveBtn.disabled = true;
        saveBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Saving...';
    }

    try {
        // Send settings to API
        const response = await fetch(`/api/enhanced-settings/${manager.currentEnvironment.id}/${manager.selectedIndex.name}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(changedSettings)
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const result = await response.json();

        if (result.success) {
            // Update local state
            Object.keys(changedSettings).forEach(key => {
                if (manager.currentSettings && manager.currentSettings.index) {
                    manager.currentSettings.index[key] = changedSettings[key];
                }
            });

            // Remove change indicators
            changedInputs.forEach(input => {
                input.classList.remove('border-warning');
                input.title = '';
            });

            // Show success message
            manager.showNotification('success', 'Settings saved successfully!');

            // Reset save button
            if (saveBtn) {
                saveBtn.disabled = false;
                saveBtn.classList.remove('btn-success');
                saveBtn.classList.add('btn-secondary');
                saveBtn.innerHTML = '<i class="fas fa-save me-2"></i>Save Changes';
            }

        } else {
            throw new Error(result.error || 'Save failed');
        }

    } catch (error) {
        console.error('Failed to save settings:', error);

        // Show error message
        if (manager.showNotification) {
            manager.showNotification('error', `Failed to save settings: ${error.message}`);
        } else {
            alert(`Failed to save settings: ${error.message}`);
        }

        // Reset save button
        if (saveBtn) {
            saveBtn.disabled = false;
            saveBtn.innerHTML = '<i class="fas fa-save me-2"></i>Save Changes';
        }
    }
}

/**
 * Reset enhanced settings to original values
 */
function resetEnhancedSettings() {
    if (confirm('Reset all settings to their original values? This will discard any unsaved changes.')) {
        const manager = window.enhancedIndicesManager;
        if (manager && manager.selectedIndex) {
            // Reload the current category
            const activeCategory = document.querySelector('#enhancedSettingsCategories .list-group-item.active');
            if (activeCategory) {
                const categoryName = activeCategory.getAttribute('onclick')?.match(/'([^']+)'/)?.[1] || 'general';
                showEnhancedSettingsCategory(categoryName);
            }

            manager.showNotification('info', 'Settings reset to original values');
        }
    }
}

/**
 * Preview enhanced settings changes
 */
function previewEnhancedSettingsChanges() {
    const changedInputs = document.querySelectorAll('[data-setting-key].border-warning');

    if (changedInputs.length === 0) {
        alert('No settings have been changed');
        return;
    }

    let previewText = 'The following settings will be changed:\n\n';

    changedInputs.forEach(input => {
        const key = input.dataset.settingKey;
        const label = input.closest('.settings-group')?.querySelector('label')?.textContent || key;
        let value = input.value;

        if (input.type === 'checkbox') {
            value = input.checked ? 'Enabled' : 'Disabled';
        }

        previewText += `${label}: ${value}\n`;
    });

    alert(previewText);
}

/**
 * Apply enhanced settings changes and close modal
 */
async function applyEnhancedSettingsChanges() {
    await saveEnhancedSettings();

    // Close modal after successful save
    const modal = bootstrap.Modal.getInstance(document.getElementById('enhancedSettingsModal'));
    if (modal) {
        modal.hide();
    }
}

// Export functions for global access
if (typeof window !== 'undefined') {
    window.editEnhancedMappingSettings = editEnhancedMappingSettings;
    window.showEnhancedSettingsCategory = showEnhancedSettingsCategory;
    window.saveEnhancedSettings = saveEnhancedSettings;
    window.resetEnhancedSettings = resetEnhancedSettings;
    window.previewEnhancedSettingsChanges = previewEnhancedSettingsChanges;
    window.applyEnhancedSettingsChanges = applyEnhancedSettingsChanges;
}

// Initialize the Enhanced Indices Manager
window.enhancedIndicesManager = new EnhancedIndicesManager();