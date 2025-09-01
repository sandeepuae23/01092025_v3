// Global variables
let environments = [];
let currentConnection = null;
let currentColumns = [];
let mappingFields = [];
let analysisSettings = { analyzer: {} };
const similarityDefinitions = {
    my_bm25: {
        label: 'BM25',
        config: { type: 'BM25', k1: 1.2, b: 0.75 }
    },
    my_classic: {
        label: 'Classic TF-IDF',
        config: { type: 'classic', discount_overlaps: true }
    },
    my_boolean: {
        label: 'Boolean',
        config: { type: 'boolean' }
    }
};
let sortable = null;

let aiSearchConfig = {
    semantic: false,
    hybrid: false,
    ai: false,
    settings: {}
};
let workflowData = {
    currentStep: 1,
    selectedEnvironment: null,
    selectedTables: [],
    tableStructures: {},
    relationships: [],
    fieldMappings: {},
    detectionResults: null
};
let workflowInitialized = false;
let workflowGeneratedMapping = null;
// Oracle-specific global variables
let queryOracleEnvironmentId = null;
let queryTables = [];
let editingVectorField = null;
let mappingOracleEnvironmentId = null;
let mappingTables = [];
let selectedOracleTable = null;
let oracleTableColumns = [];
let selectedOracleColumns = [];
// Field configuration with nested field support
let currentConfigField = null;
let enhancedFormBuilderData = {
    environment: null,
    index: null,
    availableFields: [],
    formConfig: {},
    indexMappings: {}
};

// Current field being configured
let currentFieldConfig = null;
let currentFieldName = null;
let currentMultiValueField = null;
let currentMultiValueData = {};
let currentFieldEdit = null;

// Operator options based on input type
const operatorOptions = {
    text: {
        primary: ['==', '!=', 'wildcard', 'match'],
        secondary: ['in', 'exists', 'missing']
    },
    number: {
        primary: ['==', '!=', '>', '>=', '<', '<='],
        secondary: ['in', 'exists', 'missing']
    },
    'number-range': {
        primary: ['range', 'between'],
        secondary: ['>=', '<=', 'exists', 'missing']
    },
    date: {
        primary: ['==', '!=', '>', '>=', '<', '<='],
        secondary: ['exists', 'missing']
    },
    'date-range': {
        primary: ['range', 'between'],
        secondary: ['>=', '<=', 'exists', 'missing']
    },
    dropdown: {
        primary: ['==', '!='],
        secondary: ['in', 'exists', 'missing']
    },
    checkbox: {
        primary: ['in', '=='],
        secondary: ['!=', 'match', 'exists', 'missing']
    },
    radio: {
        primary: ['==', '!='],
        secondary: ['in', 'exists', 'missing']
    }
};


// Utility functions
function showLoading(buttonId) {
    const button = document.getElementById(buttonId);
    if (button) {
        button.disabled = true;
        const originalText = button.innerHTML;
        button.setAttribute('data-original-text', originalText);
        button.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Loading...';
    }
}

function hideLoading(buttonId) {
    const button = document.getElementById(buttonId);
    if (button) {
        button.disabled = false;
        const originalText = button.getAttribute('data-original-text');
        if (originalText) {
            button.innerHTML = originalText;
            button.removeAttribute('data-original-text');
        }
    }
}

// Initialize application


// Event listeners
function setupEventListeners() {
    // Environment forms
    const oracleForm = document.getElementById('oracleEnvironmentForm');
    const elasticsearchForm = document.getElementById('elasticsearchEnvironmentForm');
    if (oracleForm) oracleForm.addEventListener('submit', handleOracleEnvironmentSubmit);
    if (elasticsearchForm) elasticsearchForm.addEventListener('submit', handleElasticsearchEnvironmentSubmit);

    // Indices & Mappings
    const indicesEnvironment = document.getElementById('indicesEnvironment');
    const connectIndicesBtn = document.getElementById('connectIndicesBtn');
    const refreshIndices = document.getElementById('refreshIndices');
    const loadMappingToBuilder = document.getElementById('loadMappingToBuilder');



    if (indicesEnvironment) indicesEnvironment.addEventListener('change', handleIndicesEnvironmentChange);
    if (connectIndicesBtn) connectIndicesBtn.addEventListener('click', handleConnectIndices);
    if (refreshIndices) refreshIndices.addEventListener('click', handleRefreshIndices);
    if (loadMappingToBuilder) loadMappingToBuilder.addEventListener('click', handleLoadMappingToBuilder);

    const savedMappingsModal = document.getElementById('savedMappingsModal');
    if (savedMappingsModal) loadMappingToBuilder.addEventListener('click', showSavedMappings);

    // Mapping builder
    const mappingEnvironment = document.getElementById('mappingEnvironment');
    const addField = document.getElementById('addField');
    const generateMapping = document.getElementById('generateMapping');
    const downloadMapping = document.getElementById('downloadMapping');
    const updateMappingBtn = document.getElementById('updateMappingButton');

    if (mappingEnvironment) mappingEnvironment.addEventListener('change', handleMappingEnvironmentChange);
    if (addField) addField.addEventListener('click', handleAddField);
    if (generateMapping) generateMapping.addEventListener('click', handleGenerateMapping);
    if (downloadMapping) downloadMapping.addEventListener('click', handleDownloadMapping);
    if (updateMappingBtn) updateMappingBtn.addEventListener('click', showUpdateMappingModal);

    const mappingTable = document.getElementById('mappingTable');
    if (mappingTable) {
        mappingTable.addEventListener('change', handleMappingTableChange);
        console.log('✅ Added event listener for main mappingTable element');
    }

    const addLogicalOperator = document.getElementById('addLogicalOperator');
    if (addLogicalOperator) {
        addLogicalOperator.addEventListener('click', handleAddLogicalOperator);
    }

    if (mappingEnvironment) mappingEnvironment.addEventListener('change', handleMappingEnvironmentChange);

    if (generateMapping) generateMapping.addEventListener('click', handleGenerateMapping);
    if (downloadMapping) downloadMapping.addEventListener('click', handleDownloadMapping);

    // Oracle Query Runner event listeners
    const connectQueryBtn = document.getElementById('connectQueryBtn');
    const executeQueryBtn = document.getElementById('executeQueryBtn');
    const clearQueryBtn = document.getElementById('clearQueryBtn');
    const formatQueryBtn = document.getElementById('formatQueryBtn');

    if (queryOracleEnvironment) queryOracleEnvironment.addEventListener('change', handleQueryOracleEnvironmentChange);
    if (connectQueryBtn) connectQueryBtn.addEventListener('click', handleConnectQueryOracle);
    if (executeQueryBtn) executeQueryBtn.addEventListener('click', handleExecuteQuery);
    if (clearQueryBtn) clearQueryBtn.addEventListener('click', handleClearQuery);
    if (formatQueryBtn) formatQueryBtn.addEventListener('click', handleFormatQuery);

    // Oracle Mapping Builder event listeners
    const mappingOracleEnvironment = document.getElementById('mappingOracleEnvironment');
    const connectMappingBtn = document.getElementById('connectMappingBtn');
    const mappingTableSelect = document.getElementById('mappingTableSelect');
    const mappingIndex = document.getElementById('mappingIndex');
    const loadColumnsBtn = document.getElementById('loadColumns');


    const loadTableStructureBtn = document.getElementById('loadTableStructureBtn');
    const generateOracleBtn = document.getElementById('generateOracleBtn');
    const oracleMappingName = document.getElementById('oracleMappingName');
    const previewOracleBtn = document.getElementById('previewOracleBtn');


    const formEnvironment = document.getElementById('formEnvironment');
    const formIndex = document.getElementById('formIndex');
    const loadIndexFields = document.getElementById('loadIndexFields');
    const saveFormConfig = document.getElementById('saveFormConfig');


    if (formEnvironment) {
        formEnvironment.addEventListener('change', function() {
            enhancedFormBuilderData.environment = this.value;
            handleFormEnvironmentChange();
        });
    }
    if (formIndex) {
        formIndex.addEventListener('change', function() {
            enhancedFormBuilderData.index = this.value;
            handleFormIndexChange();
        });
    }
    if (loadIndexFields) loadIndexFields.addEventListener('click', handleLoadIndexFields);
    if (saveFormConfig) saveFormConfig.addEventListener('click', handleSaveFormConfig);

    // Enhanced Field Configuration Event Listeners
    const fieldInputType = document.getElementById('fieldInputType');
    if (fieldInputType) {
        fieldInputType.addEventListener('change', handleInputTypeChange);
    }

    // Enhanced Form Builder Tab Initialization
    const uiFieldMappingTab = document.getElementById('ui-field-mapping-tab');
    if (uiFieldMappingTab) {
        uiFieldMappingTab.addEventListener('shown.bs.tab', function() {
            initializeEnhancedFormBuilder();
        });
    }


    if (mappingIndex) mappingIndex.addEventListener('change', handleMappingIndexChange);
    if (loadColumnsBtn) loadColumnsBtn.addEventListener('click', handleLoadColumns);
    if (mappingOracleEnvironment) mappingOracleEnvironment.addEventListener('change', handleMappingOracleEnvironmentChange);
    if (connectMappingBtn) connectMappingBtn.addEventListener('click', handleConnectMappingOracle);
    if (mappingTableSelect) mappingTableSelect.addEventListener('change', handleMappingTableChange);
    if (loadTableStructureBtn) loadTableStructureBtn.addEventListener('click', handleLoadTableStructure);
    if (generateOracleBtn) generateOracleBtn.addEventListener('click', handleGenerateOracleMapping);
    if (oracleMappingName) oracleMappingName.addEventListener('input', updateOracleMappingButtons);
    if (previewOracleBtn) previewOracleBtn.addEventListener('click', updateOracleMappingPreview);

    // Field configuration modal
    const isNested = document.getElementById('isNested');

    if (isNested) isNested.addEventListener('change', handleNestedChange);


    // Column selection controls (if they exist)
    const selectAllColumns = document.getElementById('selectAllColumns');
    const clearSelectedColumns = document.getElementById('clearSelectedColumns');
    const enableColumnSelection = document.getElementById('enableColumnSelection');
    if (selectAllColumns) selectAllColumns.addEventListener('click', handleSelectAllColumns);
    if (clearSelectedColumns) clearSelectedColumns.addEventListener('click', handleClearSelectedColumns);
    if (enableColumnSelection) enableColumnSelection.addEventListener('change', handleColumnSelectionModeChange);


    const workflowTab = document.getElementById('oracle-env-connection-tab');
    if (workflowTab) {
        workflowTab.addEventListener('shown.bs.tab', function() {
            initializeWorkflow();
        });
    }

    // Relationship type selector
    document.addEventListener('click', function(e) {
        if (e.target.classList.contains('relationship-type-option')) {
            document.querySelectorAll('.relationship-type-option').forEach(opt => opt.classList.remove('selected'));
            e.target.classList.add('selected');
        }
    });

    // Tab changes
    const mappingsListTab = document.getElementById('mappings-list-tab');
    if (mappingsListTab) mappingsListTab.addEventListener('click', loadSavedMappings);

    // Initialize saved mappings
    loadSavedMappings();

    // Initialize sortable functionality
    // initializeSortable();

    // Add preview update event listeners
    const previewTabs = document.querySelectorAll('[data-bs-toggle="tab"]');
    previewTabs.forEach(tab => {
        tab.addEventListener('shown.bs.tab', () => {
            updateElasticsearchMappingPreview();
        });
    });

    // Format and minify buttons for preview
    const formatBtn = document.getElementById('formatJsonBtn');
    const minifyBtn = document.getElementById('minifyJsonBtn');
    if (formatBtn) {
        formatBtn.addEventListener('click', () => {
            const preview = document.getElementById('mappingJson');
            if (preview && preview.textContent !== 'Add fields to see live mapping preview...') {
                try {
                    const formatted = JSON.stringify(JSON.parse(preview.textContent), null, 2);
                    preview.textContent = formatted;
                    showAlert('JSON formatted successfully', 'success');
                } catch (e) {
                    showAlert('Invalid JSON format', 'warning');
                }
            }
        });
    }
    if (minifyBtn) {
        minifyBtn.addEventListener('click', () => {
            const preview = document.getElementById('mappingJson');
            if (preview && preview.textContent !== 'Add fields to see live mapping preview...') {
                try {
                    const minified = JSON.stringify(JSON.parse(preview.textContent));
                    preview.textContent = minified;
                    showAlert('JSON minified successfully', 'success');
                } catch (e) {
                    showAlert('Invalid JSON format', 'warning');
                }
            }
        });
    }

    // Update preview when fields change
    const mappingBuilderObserver = new MutationObserver(() => {
        updateElasticsearchMappingPreview();
    });
    const mappingBuilder = document.getElementById('mappingBuilder');
    if (mappingBuilder) {
        mappingBuilderObserver.observe(mappingBuilder, { childList: true, subtree: true });
    }
}
document.addEventListener('DOMContentLoaded', function() {
    loadEnvironments();
    setupEventListeners();
    initializeSortable();
    refreshCustomAnalyzerList();
    populateAnalyzerDropdown();
    populateSimilarityDropdown();
    if (document.getElementById('workflowElasticEnvironment')) {
        initializeWorkflow();
    }
    // initializeNestedFieldDragDrop();
    // Enhanced field type change handler
    const elasticTypeSelect = document.getElementById('elasticType');
    if (elasticTypeSelect) {
        elasticTypeSelect.addEventListener('change', handleFieldTypeChange);
    }
    const addNestedBtn = document.getElementById('addNestedField');
    if (addNestedBtn) {
        addNestedBtn.addEventListener('click', function() {
            // Clear form
            document.getElementById('nestedFieldName').value = '';
            document.getElementById('nestedFieldType').value = '';
            document.getElementById('nestedFieldProperties').value = '';

            // Clear edit index
            window.currentEditIndex = null;

            // Show modal
            const modal = new bootstrap.Modal(document.getElementById('addNestedFieldModal'));
            modal.show();
        });
    }
    const saveNestedFieldBtn = document.getElementById('saveNestedField');
    if (saveNestedFieldBtn) {
        saveNestedFieldBtn.addEventListener('click', function() {
            const name = document.getElementById('nestedFieldName').value.trim();
            const type = document.getElementById('nestedFieldType').value;
            const properties = document.getElementById('nestedFieldProperties').value.trim();

            if (!name || !type) {
                showAlert('Please enter field name and type', 'warning');
                return;
            }

            const nestedField = { name, type };
            if (properties) {
                try {
                    nestedField.properties = JSON.parse(properties);
                } catch (e) {
                    showAlert('Invalid JSON in properties', 'warning');
                    return;
                }
            }

            addNestedFieldFromDrop(nestedField);

            // Clear form and close modal
            document.getElementById('nestedFieldForm').reset();
            const modal = bootstrap.Modal.getInstance(document.getElementById('addNestedFieldModal'));
            modal.hide();

            showAlert(`Nested field "${name}" added successfully`, 'success');
        });
    }

    const dragExistingBtn = document.getElementById('dragExistingField');
    if (dragExistingBtn) {
        dragExistingBtn.addEventListener('click', function() {
            const availableContainer = document.getElementById('availableFieldsForDrag');

            if (!availableContainer) {
                console.error('Available fields container not found');
                return;
            }

            if (availableContainer.style.display === 'none' || !availableContainer.style.display) {
                updateAvailableFieldsForDragging();
                availableContainer.style.display = 'block';
                this.innerHTML = '<i class="fas fa-eye-slash me-1"></i>Hide Fields';
                console.log('Showing available fields for dragging');
            } else {
                availableContainer.style.display = 'none';
                this.innerHTML = '<i class="fas fa-arrows-alt me-1"></i>Drag From Above';
                console.log('Hiding available fields for dragging');
            }
        });
    }


    const saveFieldConfigBtn = document.getElementById('saveFieldConfig');
    if (saveFieldConfigBtn) {
        // Remove any existing listeners to prevent duplicates
        saveFieldConfigBtn.replaceWith(saveFieldConfigBtn.cloneNode(true));

        const newSaveBtn = document.getElementById('saveFieldConfig');

        newSaveBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();

            console.log('Save Field Config clicked');
            handleSaveFieldConfig();
        });
    }

    // Add Nested Field Modal Event Listeners
    const enhancedTab = document.getElementById('enhanced-indices-tab');
    if (enhancedTab) {
        enhancedTab.addEventListener('shown.bs.tab', function() {
            // Ensure the Enhanced Indices Manager is properly initialized
            if (window.enhancedIndicesManager) {
                window.enhancedIndicesManager.refreshAllData();
            }
        });
    }


    const sections = ['rootFieldsSection', 'nestedFieldsSection', 'parentChildSection'];
    sections.forEach(sectionId => {
        const section = document.getElementById(sectionId);
        if (section) {
            section.addEventListener('dragover', handleDragOver);
            section.addEventListener('dragleave', (e) => {
                e.currentTarget.classList.remove('drag-over');
            });
        }
    });


    const formBuilderTab = document.getElementById('ui-field-mapping-tab');
    if (formBuilderTab) {
        formBuilderTab.addEventListener('shown.bs.tab', function() {
            initializeFormBuilder();
        });
    }

    const copyPreviewBtn = document.getElementById('copyPreviewBtn');
    if (copyPreviewBtn) {
        copyPreviewBtn.addEventListener('click', () => {
            const preview = document.getElementById('mappingJson');
            if (preview && preview.textContent) {
                copyToClipboard(preview.textContent, 'copyPreviewBtn');
            }
        });
    }


    const downloadPreviewBtn = document.getElementById('downloadPreviewBtn');
    if (downloadPreviewBtn) {
        downloadPreviewBtn.addEventListener('click', () => {
            const preview = document.getElementById('mappingJson');
            const mappingName = document.getElementById('mappingName').value.trim() || 'elasticsearch_mapping';
            if (preview && preview.textContent) {
                downloadMapping(preview.textContent, `${mappingName}.json`);
            }
        });
    }

    // Generate mapping button
    const generateMappingBtn = document.getElementById('generateMapping');
    if (generateMappingBtn) {
        generateMappingBtn.addEventListener('click', handleGenerateMapping);
    }

    // Download mapping button
    const downloadMappingBtn = document.getElementById('downloadMapping');
    if (downloadMappingBtn) {
        downloadMappingBtn.addEventListener('click', handleDownloadMapping);
    }
    // Copy preview button for Oracle mapping
    const copyOraclePreviewBtn = document.getElementById('copyOraclePreviewBtn');
    if (copyOraclePreviewBtn) {
        copyOraclePreviewBtn.addEventListener('click', () => {
            const preview = document.getElementById('oracleMappingPreview');
            if (preview && preview.textContent) {
                copyToClipboard(preview.textContent, 'copyOraclePreviewBtn');
            }
        });
    }

    // Refresh preview button for Oracle mapping
    const refreshOraclePreviewBtn = document.getElementById('refreshOraclePreviewBtn');
    if (refreshOraclePreviewBtn) {
        refreshOraclePreviewBtn.addEventListener('click', () => {
            updateOracleMappingPreview();
            showAlert('Preview refreshed successfully', 'success');
        });
    }

    // Preview button for Elasticsearch mapping
    const previewMappingBtn = document.getElementById('previewMapping');
    if (previewMappingBtn) {
        previewMappingBtn.addEventListener('click', () => {
            updateElasticsearchMappingPreview();
        });
    }

    // Format JSON button
    const formatJsonBtn = document.getElementById('formatJsonBtn');
    if (formatJsonBtn) {
        formatJsonBtn.addEventListener('click', () => {
            const preview = document.getElementById('mappingJson');
            if (preview && preview.textContent) {
                try {
                    const parsed = JSON.parse(preview.textContent);
                    preview.textContent = JSON.stringify(parsed, null, 2);
                } catch (e) {
                    showAlert('Invalid JSON format', 'warning');
                }
            }
        });
    }

    // Minify JSON button
    const minifyJsonBtn = document.getElementById('minifyJsonBtn');
    if (minifyJsonBtn) {
        minifyJsonBtn.addEventListener('click', () => {
            const preview = document.getElementById('mappingJson');
            if (preview && preview.textContent) {
                try {
                    const parsed = JSON.parse(preview.textContent);
                    preview.textContent = JSON.stringify(parsed);
                } catch (e) {
                    showAlert('Invalid JSON format', 'warning');
                }
            }
        });
    }

    // Live preview toggle
    const livePreviewToggle = document.getElementById('livePreviewToggle');
    if (livePreviewToggle) {
        livePreviewToggle.addEventListener('change', () => {
            if (livePreviewToggle.checked) {
                updateElasticsearchMappingPreview();
            }
        });
    }

});
// Environment management
async function loadEnvironments() {
    try {
        const response = await fetch('/environments');
        environments = await response.json();
        console.log('Loaded environments in loadEnvironments():', environments);
        updateEnvironmentsList();
        updateEnvironmentDropdowns();
        console.log('Called updateEnvironmentDropdowns() which should call updateOracleDropdowns()');
    } catch (error) {
        console.error('Error loading environments:', error);
        showAlert('Error loading environments: ' + error.message, 'danger');
    }
}

function updateEnvironmentsList() {
    const oracleContainer = document.getElementById('oracleEnvironmentsList');
    const elasticsearchContainer = document.getElementById('elasticsearchEnvironmentsList');

    oracleContainer.innerHTML = '';
    elasticsearchContainer.innerHTML = '';

    // Handle Oracle environments
    const oracleEnvs = environments.oracle || [];
    if (oracleEnvs.length === 0) {
        oracleContainer.innerHTML = '<div class="col-12"><div class="text-center text-muted"><i class="fas fa-database fa-2x mb-2"></i><p>No Oracle environments configured yet.</p></div></div>';
    } else {
        oracleEnvs.forEach(env => {
            const card = createEnvironmentCard(env, 'oracle');
            oracleContainer.appendChild(card);
        });
    }

    // Handle Elasticsearch environments
    const elasticsearchEnvs = environments.elasticsearch || [];
    if (elasticsearchEnvs.length === 0) {
        elasticsearchContainer.innerHTML = '<div class="col-12"><div class="text-center text-muted"><i class="fas fa-search fa-2x mb-2"></i><p>No Elasticsearch environments configured yet.</p></div></div>';
    } else {
        elasticsearchEnvs.forEach(env => {
            const card = createEnvironmentCard(env, 'elasticsearch');
            elasticsearchContainer.appendChild(card);
        });
    }
}

function createEnvironmentCard(env, type) {
    const card = document.createElement('div');
    card.className = 'col-md-12 mb-3';

    const isOracle = type === 'oracle';
    const icon = isOracle ? 'fas fa-database' : 'fas fa-search';
    const badgeClass = isOracle ? 'bg-danger' : 'bg-primary';
    const connectionUrl = isOracle ? env.url : env.host_url;

    card.innerHTML = `
        <div class="environment-card card h-100">
            <div class="card-body">
                <div class="d-flex justify-content-between align-items-start">
                    <h6 class="card-title">
                        <span class="connection-status status-pending" id="status-${type}-${env.id}"></span>
                        ${env.name}
                        <span class="badge ${badgeClass} ms-2">${type.toUpperCase()}</span>
                    </h6>
                    <button class="btn btn-outline-danger btn-sm" onclick="deleteEnvironment(${env.id}, '${type}')">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
                <p class="card-text">
                    <small class="text-muted">
                        <i class="${icon} me-1"></i>${connectionUrl}<br>
                        <i class="fas fa-user me-1"></i>${env.username || 'No authentication'}
                    </small>
                </p>
                <button class="btn btn-primary btn-sm" onclick="testConnection(${env.id}, '${type}')">
                    <i class="fas fa-plug me-1"></i>Test Connection
                </button>
            </div>
        </div>
    `;
    return card;
}

function updateEnvironmentDropdowns() {
    const dropdowns = ['indicesEnvironment', 'mappingEnvironment', 'workflowElasticEnvironment', 'dataLoadElasticEnv'];
    dropdowns.forEach(id => {
        const select = document.getElementById(id);
        if (select) {
            select.innerHTML = '<option value="">Select Environment...</option>';

            // Add Oracle environments
            if (environments.oracle && environments.oracle.length > 0) {
                if (id !== 'indicesEnvironment' && id !== 'workflowElasticEnvironment') {
                    const oracleGroup = document.createElement('optgroup');
                    oracleGroup.label = 'Oracle Environments';
                    environments.oracle.forEach(env => {
                        const option = document.createElement('option');
                        option.value = `oracle-${env.id}`;
                        option.textContent = `${env.name} (Oracle)`;
                        oracleGroup.appendChild(option);
                    });
                    select.appendChild(oracleGroup);
                }
            }

            // Add Elasticsearch environments
            if (environments.elasticsearch && environments.elasticsearch.length > 0) {
                const elasticsearchGroup = document.createElement('optgroup');
                elasticsearchGroup.label = 'Elasticsearch Environments';
                environments.elasticsearch.forEach(env => {
                    const option = document.createElement('option');
                    if (id === 'indicesEnvironment' || id === 'workflowElasticEnvironment') {
                        option.value = env.id;
                    } else {
                        option.value = `elasticsearch-${env.id}`;
                    }
                    option.textContent = `${env.name} (Elasticsearch)`;
                    elasticsearchGroup.appendChild(option);
                });
                select.appendChild(elasticsearchGroup);
            }
        }
    });

    // Update Oracle-specific dropdowns
    updateOracleDropdowns();
}

function updateOracleDropdowns() {
    console.log('updateOracleDropdowns called, environments:', environments);

    // Update Oracle Query Runner dropdown
    const querySelect = document.getElementById('queryOracleEnvironment');
    if (querySelect) {
        querySelect.innerHTML = '<option value="">Select Oracle Environment...</option>';
        if (environments.oracle && environments.oracle.length > 0) {
            console.log('Populating Oracle Query Runner dropdown with', environments.oracle.length, 'environments');
            environments.oracle.forEach(env => {
                const option = document.createElement('option');
                option.value = env.id;
                option.textContent = env.name;
                querySelect.appendChild(option);
            });
        } else {
            console.log('No Oracle environments found for Query Runner dropdown');
        }
    } else {
        console.log('queryOracleEnvironment element not found');
    }

    // Update Oracle Mapping Builder dropdown
    const mappingSelect = document.getElementById('mappingOracleEnvironment');
    if (mappingSelect) {
        mappingSelect.innerHTML = '<option value="">Select Oracle Environment...</option>';
        if (environments.oracle && environments.oracle.length > 0) {
            console.log('Populating Oracle Mapping Builder dropdown with', environments.oracle.length, 'environments');
            environments.oracle.forEach(env => {
                const option = document.createElement('option');
                option.value = env.id;
                option.textContent = env.name;
                mappingSelect.appendChild(option);
            });
        } else {
            console.log('No Oracle environments found for Mapping Builder dropdown');
        }
    } else {
        console.log('mappingOracleEnvironment element not found');
    }

    // Update Data Load Oracle environment dropdown
    const dataLoadSelect = document.getElementById('dataLoadOracleEnv');
    if (dataLoadSelect) {
        dataLoadSelect.innerHTML = '<option value="">Select Oracle Environment...</option>';
        if (environments.oracle && environments.oracle.length > 0) {
            environments.oracle.forEach(env => {
                const option = document.createElement('option');
                option.value = env.id;
                option.textContent = env.name;
                dataLoadSelect.appendChild(option);
            });
        }
    }
}

async function handleOracleEnvironmentSubmit(e) {
    e.preventDefault();

    const formData = new FormData();
    formData.append('name', document.getElementById('oracleEnvName').value);
    formData.append('url', document.getElementById('oracleEnvUrl').value);
    formData.append('username', document.getElementById('oracleEnvUsername').value);
    formData.append('password', document.getElementById('oracleEnvPassword').value);

    try {
        const response = await fetch('/environments/oracle', {
            method: 'POST',
            body: formData
        });

        if (response.ok) {
            document.getElementById('oracleEnvironmentForm').reset();
            showAlert('Oracle environment saved successfully!', 'success');
            loadEnvironments();
        } else {
            throw new Error('Failed to save Oracle environment');
        }
    } catch (error) {
        showAlert('Error saving Oracle environment: ' + error.message, 'danger');
    }
}

async function handleElasticsearchEnvironmentSubmit(e) {
    e.preventDefault();

    const formData = new FormData();
    formData.append('name', document.getElementById('elasticEnvName').value);
    formData.append('host_url', document.getElementById('elasticEnvUrl').value);
    formData.append('username', document.getElementById('elasticEnvUsername').value || '');
    formData.append('password', document.getElementById('elasticEnvPassword').value || '');

    try {
        const response = await fetch('/environments/elasticsearch', {
            method: 'POST',
            body: formData
        });

        if (response.ok) {
            document.getElementById('elasticsearchEnvironmentForm').reset();
            showAlert('Elasticsearch environment saved successfully!', 'success');
            loadEnvironments();
        } else {
            throw new Error('Failed to save Elasticsearch environment');
        }
    } catch (error) {
        showAlert('Error saving Elasticsearch environment: ' + error.message, 'danger');
    }
}

async function deleteEnvironment(envId, envType) {
    if (!confirm(`Are you sure you want to delete this ${envType} environment?`)) return;

    try {
        const response = await fetch(`/environments/${envType}/${envId}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            showAlert(`${envType} environment deleted successfully!`, 'success');
            loadEnvironments();
        } else {
            throw new Error(`Failed to delete ${envType} environment`);
        }
    } catch (error) {
        showAlert(`Error deleting ${envType} environment: ` + error.message, 'danger');
    }
}

async function testConnection(envId, envType) {
    const statusElement = document.getElementById(`status-${envType}-${envId}`);
    statusElement.className = 'connection-status status-pending';

    try {
        const response = await fetch(`/test-connection/${envType}/${envId}`, {
            method: 'POST'
        });
        const result = await response.json();

        if (result.success) {
            statusElement.className = 'connection-status status-success';
            showAlert(`${envType} connection successful!`, 'success');
        } else {
            statusElement.className = 'connection-status status-error';
            showAlert(`${envType} connection failed: ` + result.message, 'danger');
        }
    } catch (error) {
        statusElement.className = 'connection-status status-error';
        showAlert(`${envType} connection error: ` + error.message, 'danger');
    }
}

// Indices & Mappings
function handleIndicesEnvironmentChange() {
    const envId = document.getElementById('indicesEnvironment').value;
    document.getElementById('connectIndicesBtn').disabled = !envId;
    document.getElementById('indicesCard').style.display = 'none';
    document.getElementById('refreshIndices').disabled = true;
    document.getElementById('loadMappingToBuilder').disabled = true;
}

async function handleConnectIndices() {
    const envId = document.getElementById('indicesEnvironment').value;
    if (!envId) return;

    const button = document.getElementById('connectIndicesBtn');
    button.disabled = true;
    button.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Connecting...';

    try {
        // Test connection
        const testResponse = await fetch(`/test-connection/elasticsearch/${envId}`, {
            method: 'POST'
        });
        const testResult = await testResponse.json();

        if (!testResult.success) {
            throw new Error(testResult.message);
        }

        // Load indices
        await loadIndices(envId);

        document.getElementById('indicesCard').style.display = 'block';
        document.getElementById('refreshIndices').disabled = false;

        showAlert(`Connected to ${testResult.cluster_name || 'Elasticsearch'}!`, 'success');
    } catch (error) {
        showAlert('Connection failed: ' + error.message, 'danger');
    } finally {
        button.disabled = false;
        button.innerHTML = '<i class="fas fa-plug me-2"></i>Connect';
    }
}

async function loadIndices(envId) {
    try {
        const response = await fetch(`/indices/${envId}`);
        const indices = await response.json();

        updateIndicesList(indices);
        updateAvailableIndices(indices);
    } catch (error) {
        showAlert('Error loading indices: ' + error.message, 'danger');
    }
}

function updateIndicesList(indices) {
    const container = document.getElementById('indicesList');
    container.innerHTML = '';

    if (indices.length === 0) {
        container.innerHTML = '<div class="list-group-item text-muted">No indices found</div>';
        return;
    }

    indices.forEach(index => {
        const item = document.createElement('div');
        item.className = 'list-group-item list-group-item-action';
        item.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <h6 class="mb-1">${index.index}</h6>
                    <small class="text-muted">
                        ${index['docs.count'] || 0} docs, ${index['store.size'] || '0b'}
                    </small>
                </div>
                <button class="btn btn-sm btn-outline-primary" onclick="loadIndexMapping('${index.index}')">
                    View Mapping
                </button>
            </div>
        `;
        container.appendChild(item);
    });
}

function updateAvailableIndices(indices) {
    const container = document.getElementById('availableIndices');
    container.innerHTML = '';

    if (indices.length === 0) {
        container.innerHTML = '<p class="text-muted">No indices found</p>';
        return;
    }

    indices.forEach(index => {
        const button = document.createElement('button');
        button.className = 'btn btn-outline-primary btn-sm m-1';
        button.textContent = index.index;
        button.onclick = () => loadIndexMapping(index.index);
        container.appendChild(button);
    });
}

async function loadIndexMapping(indexName) {
    const envId = document.getElementById('indicesEnvironment').value;
    if (!envId) return;

    try {
        console.log(''+indexName);
        const response = await fetch(`/mapping/${envId}/${indexName}`);
        const mapping = await response.json();

        const container = document.getElementById('indexMappingDisplay');
        container.innerHTML = `<pre>${JSON.stringify(mapping, null, 2)}</pre>`;

        document.getElementById('loadMappingToBuilder').disabled = false;
        document.getElementById('loadMappingToBuilder').onclick = () => loadMappingToMappingBuilder(mapping);
        const contentType = response.headers.get('content-type');
        if (!response.ok) {
            // Handle error responses
            if (contentType && contentType.includes('application/json')) {

            }
        }
    } catch (error) {
        showAlert('Error loading mapping: ' + error.message, 'danger');
    }
}

async function handleRefreshIndices() {
    const envId = document.getElementById('indicesEnvironment').value;
    if (!envId) return;

    await loadIndices(envId);
    showAlert('Indices refreshed!', 'success');
}

function handleLoadMappingToBuilder() {
    // Switch to mapping builder tab
    const mappingTab = document.getElementById('mapping-tab');
    mappingTab.click();

    showAlert('Mapping loaded to builder!', 'success');
}

// Mapping Builder
// Removed duplicate function - kept the comprehensive version later in the file

function handleAddField() {
    const fieldName = prompt('Enter field name:');
    if (!fieldName) return;

    const fieldType = prompt('Enter field type (text, keyword, long, date, etc.):') || 'text';

    addFieldToBuilder(fieldName, fieldType);
}

function addQuickField(name, type) {
    addFieldToBuilder(name, type);
}

function addFieldToBuilder(name, type) {
    const builder = document.getElementById('mappingBuilder');

    // Remove placeholder text if this is the first field
    if (builder.querySelector('.text-center')) {
        builder.innerHTML = '';
    }

    const fieldItem = document.createElement('div');
    fieldItem.className = 'mapping-field-item p-3 mb-2 border rounded';
    fieldItem.draggable = true;
    fieldItem.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
            <div>
                <strong>${name}</strong>
                <span class="badge bg-secondary ms-2">${type}</span>
            </div>
            <div>
                <button class="btn btn-sm btn-outline-primary me-2" onclick="configureField('${name}')">
                    <i class="fas fa-cog"></i>
                </button>
                <button class="btn btn-sm btn-outline-danger" onclick="removeField(this)">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </div>
    `;

    builder.appendChild(fieldItem);

    // Add to mappingFields array
    mappingFields.push({
        field_name: name,
        field_type: type,
        properties: {},
        nested_fields: []
    });

    updateGenerateButton();
}

function removeField(button) {
    const fieldItem = button.closest('.mapping-field-item');
    const fieldName = fieldItem.querySelector('strong').textContent;

    // Remove from DOM
    fieldItem.remove();

    // Remove from mappingFields array
    mappingFields = mappingFields.filter(f => f.field_name !== fieldName);

    // Show placeholder if no fields left
    const builder = document.getElementById('mappingBuilder');
    if (builder.children.length === 0) {
        builder.innerHTML = `
            <div class="text-center text-muted">
                <i class="fas fa-arrow-up fa-2x mb-3"></i>
                <p>Drag and drop fields here to build your Elasticsearch mapping</p>
                <p>Use the field configuration panel to customize types and properties</p>
            </div>
        `;
    }

    updateGenerateButton();
}

function updateGenerateButton() {
    const envId = document.getElementById('mappingEnvironment').value;
    const hasFields = mappingFields.length > 0;
    document.getElementById('generateMapping').disabled = !envId || !hasFields;
}

// Removed duplicate handleGenerateMapping - using enhanced version later in file

// Removed duplicate handleDownloadMapping function

function updateTablesList(tables) {
    const container = document.getElementById('tablesList');
    container.innerHTML = '';

    tables.forEach(table => {
        const item = document.createElement('a');
        item.className = 'list-group-item list-group-item-action';
        item.href = '#';
        item.textContent = table;
        item.onclick = () => insertTableName(table);
        container.appendChild(item);
    });
}

// Removed duplicate insertTableName function

async function handleQueryExecute() {
    const query = document.getElementById('queryEditor').value.trim();
    if (!query || !currentConnection) return;

    const button = document.getElementById('executeQuery');
    const spinner = button.querySelector('.loading-spinner');

    button.disabled = true;
    spinner.classList.add('show');

    try {
        const formData = new FormData();
        formData.append('query', query);

        const response = await fetch(`/execute-query/${currentConnection}`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            throw new Error('Query execution failed');
        }

        const result = await response.json();
        displayQueryResults(result);
        document.getElementById('queryResults').style.display = 'block';

    } catch (error) {
        showAlert('Query error: ' + error.message, 'danger');
    } finally {
        button.disabled = false;
        spinner.classList.remove('show');
    }
}

// This function was moved to avoid duplication

function handleQueryClear() {
    const queryEditor = document.getElementById('queryEditor');
    const queryResults = document.getElementById('queryResults');

    if (queryEditor) queryEditor.value = '';
    if (queryResults) queryResults.style.display = 'none';
}

// Mapping builder
function handleMappingEnvironmentChange() {
    const envValue = document.getElementById('mappingEnvironment').value;
    const tableSelect = document.getElementById('mappingTable');
    const indexSelect = document.getElementById('mappingIndex');
    const oracleSection = document.getElementById('oracleTableSection');
    const elasticsearchSection = document.getElementById('elasticsearchIndexSection');
    console.log('Elements found:', {
        tableSelect: !!tableSelect,
        indexSelect: !!indexSelect,
        oracleSection: !!oracleSection,
        elasticsearchSection: !!elasticsearchSection
    });
    if (envValue) {
        // Parse environment type and ID
        const [envType, envId] = envValue.split('-');
        console.log('Parsed environment:', { envType, envId });

        if (envType === 'oracle') {
            // Show table selection for Oracle
            if (oracleSection) {
                oracleSection.style.display = 'block';
                console.log('Oracle section shown');
            }
            if (elasticsearchSection) {
                elasticsearchSection.style.display = 'none';
                console.log('Elasticsearch section hidden');
            }

            if (tableSelect) {
                tableSelect.disabled = false;
                loadTablesForMapping(envId, 'oracle');
                console.log('Table select enabled, loading tables');
            }
        } else if (envType === 'elasticsearch') {
            // Show index selection for Elasticsearch
            if (oracleSection) {
                oracleSection.style.display = 'none';
                console.log('Oracle section hidden');
            }
            if (elasticsearchSection) {
                elasticsearchSection.style.display = 'block';
                console.log('Elasticsearch section shown');
            }

            if (indexSelect) {
                indexSelect.disabled = false;
                loadIndicesForMapping(envId);
                console.log('Index select enabled, loading indices');
            } else {
                console.error('indexSelect is null after showing elasticsearch section');
            }
        }
    } else {
        // Reset both selectors and hide sections
        if (oracleSection) oracleSection.style.display = 'none';
        if (elasticsearchSection) elasticsearchSection.style.display = 'none';

        if (tableSelect) {
            tableSelect.disabled = true;
            tableSelect.innerHTML = '<option value="">Select Table...</option>';
        }
        if (indexSelect) {
            indexSelect.disabled = true;
            indexSelect.innerHTML = '<option value="">Select Index...</option>';
        }
    }

    // Reset downstream elements
    const loadColumnsBtn = document.getElementById('loadColumns');
    const columnsCard = document.getElementById('columnsCard');
    if (loadColumnsBtn) loadColumnsBtn.disabled = true;
    if (columnsCard) columnsCard.style.display = 'none';
}



async function loadIndicesForMapping(envId) {
    try {
        console.log('Loading indices for environment:', envId);
        const response = await fetch(`/indices/${envId}`);
        const indices = await response.json();
        console.log('Indices loaded:', indices);

        const select = document.getElementById('mappingIndex');
        console.log('mappingIndex element:', select);

        if (select) {
            select.innerHTML = '<option value="">Select Index...</option>';

            if (Array.isArray(indices)) {
                indices.forEach(index => {
                    console.log('Indices loaded:::::', index);
                    const option = document.createElement('option');
                    option.value = index.index;
                    option.textContent = index.index;
                    select.appendChild(option);
                });
                console.log(`Added ${indices.length} indices to dropdown`);
            } else {
                console.error('Indices is not an array:', indices);
            }
        } else {
            console.error('mappingIndex element not found in DOM');

        }
    } catch (error) {
        console.error('Error loading indices:', error);
        showAlert('Error loading indices: ' + error.message, 'danger');
    }
}


async function loadTablesForMapping(envId, envType = 'oracle') {
    try {
        let endpoint;
        if (envType === 'oracle') {
            endpoint = `/tables/${envId}`;
        } else {
            endpoint = `/indices/${envId}`;
        }

        const response = await fetch(endpoint);
        const result = await response.json();

        const select = document.getElementById('mappingTable');
        if (select) {
            select.innerHTML = '<option value="">Select Table...</option>';

            if (envType === 'oracle' && result.tables) {
                result.tables.forEach(table => {
                    const option = document.createElement('option');
                    option.value = table.table_name;
                    option.textContent = table.table_name;
                    select.appendChild(option);
                });
            }
        }
    } catch (error) {
        showAlert('Error loading tables: ' + error.message, 'danger');
    }
}
async function loadTablesForMapping_v1(envId) {
    try {
        const response = await fetch(`/tables/${envId}`);
        const result = await response.json();

        const select = document.getElementById('mappingTable');
        select.innerHTML = '<option value="">Select Table...</option>';

        result.tables.forEach(table => {
            const option = document.createElement('option');
            option.value = table.table_name;
            option.textContent = table.table_name;
            select.appendChild(option);
        });
    } catch (error) {
        showAlert('Error loading tables: ' + error.message, 'danger');
    }
}


function handleMappingIndexChange() {
    const indexSelect = document.getElementById('mappingIndex');
    const loadColumnsBtn = document.getElementById('loadColumns');

    if (indexSelect && loadColumnsBtn) {
        const indexName = indexSelect.value;
        loadColumnsBtn.disabled = !indexName;
        console.log('Index changed to:', indexName, 'Load button disabled:', !indexName);
    } else {
        console.error('Elements not found:', { indexSelect: !!indexSelect, loadColumnsBtn: !!loadColumnsBtn });
    }
}
async function handleLoadColumns() {
    const envValue = document.getElementById('mappingEnvironment').value;
    const tableName = document.getElementById('mappingTable')?.value;
    const indexName = document.getElementById('mappingIndex')?.value;

    if (!envValue) return;

    const [envType, envId] = envValue.split('-');

    try {
        let response, result;

        if (envType === 'oracle' && tableName) {
            // Load Oracle table columns
            response = await fetch(`/columns/${envId}/${tableName}`);
            result = await response.json();
            currentColumns = result.columns || [];
        } else if (envType === 'elasticsearch' && indexName) {
            // Load Elasticsearch index mapping
            console.log(indexName);
            console.log(envType);
            response = await fetch(`/mapping/${envId}/${indexName}`);
            result = await response.json();

            // Convert mapping to columns format
            const mappingKey = Object.keys(result.mapping)[0]; // e.g., 'test2'
            const properties = result.mapping?.[mappingKey]?.mappings?.properties;

            function extractFields(properties, parentKey = '') {
                let fields = [];

                for (const [key, value] of Object.entries(properties)) {
                    const fullKey = parentKey ? `${parentKey}.${key}` : key;

                    if (value.properties) {
                        // It's a nested object — recurse
                        fields = fields.concat(extractFields(value.properties, fullKey));
                    } else {
                        // It's a primitive field
                        fields.push({
                            name: fullKey,
                            type: value.type || 'object',
                            nullable: true
                        });
                    }
                }

                return fields;
            }
            currentColumns = properties ? extractFields(properties) : [];
        } else {
            showAlert('Please select a table or index first', 'warning');
            return;
        }

        updateColumnsList();
        document.getElementById('columnsCard').style.display = 'block';
        document.getElementById('generateMapping').disabled = false;

    } catch (error) {
        showAlert('Error loading columns: ' + error.message, 'danger');
    }
}

// Removed duplicate handleMappingTableChange function

async function handleLoadColumns_v1() {
    const envId = document.getElementById('mappingEnvironment').value;
    const tableName = document.getElementById('mappingTable').value;

    if (!envId || !tableName) return;

    try {
        const response = await fetch(`/columns/${envId}/${tableName}`);
        const result = await response.json();

        currentColumns = result.columns;
        updateColumnsList();
        document.getElementById('columnsCard').style.display = 'block';
        document.getElementById('generateMapping').disabled = false;

    } catch (error) {
        showAlert('Error loading columns: ' + error.message, 'danger');
    }
}

function updateColumnsList() {
    const container = document.getElementById('columnsList');
    const enableSelection = document.getElementById('enableColumnSelection').checked;
    container.innerHTML = '';

    currentColumns.forEach((column, index) => {
        const item = document.createElement('div');
        item.className = 'column-item';
        item.innerHTML = `
            <div class="d-flex justify-content-between align-items-center p-2 border rounded mb-2 bg-light column-entry" 
                 data-column='${JSON.stringify(column)}' data-column-index="${index}">
                <div class="d-flex align-items-center">
                    ${enableSelection ? `
                        <div class="form-check me-3">
                            <input class="form-check-input column-checkbox" type="checkbox" 
                                   id="col_${index}" data-column-name="${column.name}">
                        </div>
                    ` : ''}
                    <div>
                        <strong>${column.name}</strong>
                        <br>
                        <small class="text-muted">${column.type} ${column.length ? `(${column.length})` : ''}</small>
                    </div>
                </div>
                <div class="d-flex align-items-center">
                    ${enableSelection ? `
                        <button class="btn btn-sm btn-outline-success me-2 add-column-btn" 
                                data-column-index="${index}" title="Add to mapping">
                            <i class="fas fa-plus"></i>
                        </button>
                    ` : ''}
                    <i class="fas fa-grip-vertical text-muted drag-handle"></i>
                </div>
            </div>
        `;
        container.appendChild(item);
    });

    // Add event listeners for checkboxes and add buttons
    if (enableSelection) {
        container.querySelectorAll('.column-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', handleColumnCheckboxChange);
        });

        container.querySelectorAll('.add-column-btn').forEach(btn => {
            btn.addEventListener('click', handleAddSingleColumn);
        });
    }

    // Initialize sortable for the columns list
    initializeColumnsSortable();

    // Update selection controls
    updateSelectionControls();
}

// Drag and drop functionality
function initializeSortable() {
    const mappingBuilder = document.getElementById('mappingBuilder');

    if (sortable) {
        sortable.destroy();
    }

    sortable = new Sortable(mappingBuilder, {
        group: {
            name: 'mapping',
            pull: false,
            put: ['columns']
        },
        animation: 150,
        ghostClass: 'sortable-ghost',
        chosenClass: 'sortable-chosen',
        dragClass: 'sortable-drag',
        onAdd: function(evt) {
            const item = evt.item;
            if (item.dataset.column) {
                const columnData = JSON.parse(item.dataset.column);
                addFieldToMapping(columnData, evt.newIndex);
            }
        },
        onUpdate: function(evt) {
            updateMappingFieldsOrder();
        }
    });
}

function initializeColumnsSortable() {
    const columnsList = document.getElementById('columnsList');

    new Sortable(columnsList, {
        group: {
            name: 'columns',
            pull: 'clone',
            put: false
        },
        sort: false,
        animation: 150,
        ghostClass: 'sortable-ghost',
        chosenClass: 'sortable-chosen',
        dragClass: 'sortable-drag'
    });
}

// Column selection handlers
function handleSelectAllColumns() {
    // Add all columns to mapping
    currentColumns.forEach(column => {
        // Check if column is already in mapping
        const exists = mappingFields.find(f => f.field_name === column.name);
        if (!exists) {
            addFieldToMapping(column, mappingFields.length);
        }
    });

    // Update checkboxes
    document.querySelectorAll('.column-checkbox').forEach(checkbox => {
        checkbox.checked = true;
    });

    updateSelectionControls();
    showAlert('All columns added to mapping', 'success');
}

function handleClearSelectedColumns() {
    // Clear all fields from mapping
    mappingFields = [];
    document.getElementById('mappingBuilder').innerHTML = `
        <div class="text-center text-muted">
            <i class="fas fa-arrow-up fa-2x mb-3"></i>
            <p>Drag and drop fields here to build your Elasticsearch mapping</p>
            <p>Use the field configuration panel to customize types and properties</p>
        </div>
    `;

    // Uncheck all checkboxes
    document.querySelectorAll('.column-checkbox').forEach(checkbox => {
        checkbox.checked = false;
    });

    updateSelectionControls();
    showAlert('All columns removed from mapping', 'info');
}

function handleColumnSelectionModeChange() {
    updateColumnsList();
    updateSelectionControls();
}

function handleColumnCheckboxChange(event) {
    const checkbox = event.target;
    const columnName = checkbox.dataset.columnName;
    const column = currentColumns.find(c => c.name === columnName);

    if (checkbox.checked) {
        // Add column to mapping if not already present
        const exists = mappingFields.find(f => f.field_name === columnName);
        if (!exists && column) {
            addFieldToMapping(column, mappingFields.length);
        }
    } else {
        // Remove column from mapping
        const fieldId = `field_${columnName}`;
        removeField(fieldId);
    }

    updateSelectionControls();
}

function handleAddSingleColumn(event) {
    const button = event.target.closest('.add-column-btn');
    const columnIndex = parseInt(button.dataset.columnIndex);
    const column = currentColumns[columnIndex];

    // Check if column is already in mapping
    const exists = mappingFields.find(f => f.field_name === column.name);
    if (!exists) {
        addFieldToMapping(column, mappingFields.length);

        // Update corresponding checkbox
        const checkbox = document.querySelector(`input[data-column-name="${column.name}"]`);
        if (checkbox) {
            checkbox.checked = true;
        }

        updateSelectionControls();
        showAlert(`Column "${column.name}" added to mapping`, 'success');
    } else {
        showAlert(`Column "${column.name}" is already in mapping`, 'warning');
    }
}

function updateSelectionControls() {
    const totalColumns = currentColumns.length;
    const selectedColumns = mappingFields.length;
    const enableSelection = document.getElementById('enableColumnSelection')?.checked;

    // Update button states
    const selectAllBtn = document.getElementById('selectAllColumns');
    const clearAllBtn = document.getElementById('clearSelectedColumns');

    if (selectAllBtn) {
        selectAllBtn.disabled = !enableSelection || selectedColumns === totalColumns;
        selectAllBtn.innerHTML = selectedColumns === totalColumns ?
            '<i class="fas fa-check me-1"></i>All Selected' :
            '<i class="fas fa-check-double me-1"></i>Select All';
    }

    if (clearAllBtn) {
        clearAllBtn.disabled = !enableSelection || selectedColumns === 0;
    }

    // Update column checkboxes based on current mapping
    if (enableSelection) {
        document.querySelectorAll('.column-checkbox').forEach(checkbox => {
            const columnName = checkbox.dataset.columnName;
            const isInMapping = mappingFields.find(f => f.field_name === columnName);
            checkbox.checked = !!isInMapping;
        });
    }
}

function addFieldToMapping(columnData, index) {
    const fieldId = `field_${columnData.name}`;

    // Remove the dragged element (it was added by sortable)
    const draggedElement = document.querySelector(`[data-column='${JSON.stringify(columnData)}']`);
    if (draggedElement && draggedElement.parentNode.id === 'mappingBuilder') {
        draggedElement.remove();
    }

    // Create proper field item
    const fieldItem = document.createElement('div');
    fieldItem.className = 'field-item';
    fieldItem.id = fieldId;
    fieldItem.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
            <div>
                <strong>${columnData.name}</strong>
                <br>
                <small class="text-muted">Oracle: ${columnData.type} → Elastic: <span class="elastic-type">text</span></small>
                <div class="field-config mt-2" style="display: none;">
                    <span class="badge bg-secondary nested-badge" style="display: none;">Nested</span>
                    <span class="badge bg-info parent-badge" style="display: none;"></span>
                </div>
            </div>
            <div>
                <button class="btn btn-sm btn-outline-primary me-2" onclick="configureField('${columnData.name}')">
                    <i class="fas fa-cog"></i>
                </button>
                <button class="btn btn-sm btn-outline-danger" onclick="removeField('${fieldId}')">
                    <i class="fas fa-trash"></i>
                </button>
                <i class="fas fa-grip-vertical text-muted ms-2"></i>
            </div>
        </div>
    `;

    const mappingBuilderObserver = new MutationObserver(() => {
        updateElasticsearchMappingPreview();
    });
    // Insert at the correct position
    const mappingBuilder = document.getElementById('mappingBuilder');
    const children = Array.from(mappingBuilder.children);
    if (index >= children.length) {
        mappingBuilder.appendChild(fieldItem);
    } else {
        mappingBuilder.insertBefore(fieldItem, children[index]);
    }

    const mappingTable = document.getElementById('mappingTable');
    if (mappingTable) {
        mappingTable.addEventListener('change', handleMappingTableChange);
        console.log('✅ Added event listener for main mappingTable element');
    } else {
        console.log('⚠️ Main mappingTable element not found during setup');
    }
    // Add to mapping fields array
    const mappingField = {
        field_name: columnData.name.toLowerCase(),
        oracle_type: columnData.type,
        elastic_type: oracleToElasticType(columnData.type),
        ui_component_type: 'text_box',
        is_nested: false,
        parent_field: null,
        properties: {},
        source_index: null,
        key_field: null,
        value_field: null
    };

    mappingFields.push(mappingField);

    // Update the empty state and selection controls
    updateMappingBuilderEmptyState();
    updateSelectionControls();
}

// Removed duplicate removeField function - using the comprehensive version

function updateMappingBuilderEmptyState() {
    const mappingBuilder = document.getElementById('mappingBuilder');
    const hasFields = mappingBuilder.querySelectorAll('.field-item').length > 0;

    if (!hasFields) {
        mappingBuilder.innerHTML = `
            <div class="text-center text-muted">
                <i class="fas fa-arrow-up fa-2x mb-3"></i>
                <p>Drag and drop fields here to build your Elasticsearch mapping</p>
                <p>Use the field configuration panel to customize types and properties</p>
            </div>
        `;
    }
}

function updateMappingFieldsOrder() {
    const fieldElements = document.querySelectorAll('#mappingBuilder .field-item');
    const newOrder = [];

    fieldElements.forEach(element => {
        const fieldName = element.querySelector('strong').textContent;
        const field = mappingFields.find(f => f.field_name === fieldName);
        if (field) {
            newOrder.push(field);
        }
    });

    mappingFields = newOrder;
}



function configureField(fieldName) {
    console.log('Configure field called for:', fieldName.toLowerCase());

    const field = mappingFields.find(f => f.field_name === fieldName.toLowerCase());
    if (!field) {
        console.error('Field not found:', fieldName.toLowerCase());
        showAlert('Field not found', 'danger');
        return;
    }

    currentConfigField = field;
    currentFieldName = fieldName.toLowerCase();

    // Populate basic configuration
    document.getElementById('fieldName').value = field.field_name;
    document.getElementById('elasticType').value = field.elastic_type || field.field_type;

    const isNestedCheckbox = document.getElementById('isNested');
    if (isNestedCheckbox) {
        isNestedCheckbox.checked = field.is_nested || false;
    }

    const fieldPropertiesTextarea = document.getElementById('fieldProperties');
    if (fieldPropertiesTextarea) {
        fieldPropertiesTextarea.value = JSON.stringify(field.properties || {}, null, 2);
    }

    // Populate UI component configuration
    const uiComponentSelect = document.getElementById('uiComponentType');
    if (uiComponentSelect) {
        uiComponentSelect.value = field.ui_component_type || 'text_box';
    }

    // Handle dropdown configuration
    if (field.ui_component_type === 'dropdown') {
        const dropdownConfig = document.getElementById('dropdownConfig');
        if (dropdownConfig) {
            dropdownConfig.style.display = 'block';

            const sourceIndex = document.getElementById('sourceIndex');
            const keyField = document.getElementById('keyField');
            const valueField = document.getElementById('valueField');

            if (sourceIndex) sourceIndex.value = field.source_index || '';
            if (keyField) keyField.value = field.key_field || '';
            if (valueField) valueField.value = field.value_field || '';
        }
    } else {
        const dropdownConfig = document.getElementById('dropdownConfig');
        if (dropdownConfig) dropdownConfig.style.display = 'none';
    }

    // Handle enhanced field options
    populateEnhancedFieldOptions(field);

    // Update parent field dropdown
    updateParentFieldDropdown(fieldName);

    if (field.parent_field) {
        const parentFieldSelect = document.getElementById('parentFieldv1');
        if (parentFieldSelect) {
            parentFieldSelect.value = field.parent_field;
        }
    }

    // Initialize nested fields if they don't exist
    if (!field.nested_fields) {
        field.nested_fields = [];
    }

    // Load nested fields
    loadNestedFields(field.nested_fields);

    handleNestedChange();

    // Show available fields for dragging
    updateAvailableFieldsForDragging();

    // Initialize drag and drop
    initializeNestedFieldDragDrop();

    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('fieldConfigModal'));
    modal.show();

    console.log('Field configuration modal opened for:', fieldName);
}

function updateParentFieldDropdown(currentFieldName) {
    const select = document.getElementById('parentFieldv1');
    select.innerHTML = '<option value="">Select parent field...</option>';

    mappingFields.forEach(field => {
        if (field.field_name !== currentFieldName) {
            const option = document.createElement('option');
            option.value = field.field_name;
            option.textContent = field.field_name;
            select.appendChild(option);
        }
    });
}

function handleNestedChange() {
    const isNested = document.getElementById('isNested').checked;
    const parentFieldGroup = document.getElementById('parentFieldGroup');
    //parentFieldGroup.style.display = isNested ? 'block' : 'none';
}

function handleSaveFieldConfig() {
    console.log('handleSaveFieldConfig called');

    try {
        const fieldName = document.getElementById('fieldName').value;
        const elasticType = document.getElementById('elasticType').value;
        const isNested = document.getElementById('isNested').checked;
        const parentFieldv1 = document.getElementById('parentFieldv1').value;
        const propertiesText = document.getElementById('fieldProperties').value;
        const container = document.getElementById('nestedFieldsList');

        console.log('Field config data:', { fieldName, elasticType, isNested, parentFieldv1 });
        console.log('Field config data:', { container });
        console.log('currentConfigField :',  currentConfigField.nested_fields);

        if (!fieldName || !elasticType) {
            showAlert('Field name and type are required', 'warning');
            return;
        }

        let properties = {};
        try {
            if (propertiesText.trim()) {
                properties = JSON.parse(propertiesText);
            }
        } catch (error) {
            showAlert('Invalid JSON in properties field', 'danger');
            return;
        }

        // Get UI component configuration
        const uiComponentType = document.getElementById('uiComponentType').value;
        const sourceIndex = document.getElementById('sourceIndex').value;
        const keyField = document.getElementById('keyField').value;
        const valueField = document.getElementById('valueField').value;

        // Collect enhanced field options
        const enhancedProps = {};

        // Text field analyzer
        const analyzerElement = document.getElementById('analyzer');
        if (elasticType === 'text' && analyzerElement) {
            if (analyzerElement.value) {
                enhancedProps.analyzer = analyzerElement.value;
            } else {
                delete properties.analyzer;
            }
        }

        const similarityElement = document.getElementById('similarity');
        if (elasticType === 'text' && similarityElement) {
            if (similarityElement.value) {
                enhancedProps.similarity = similarityElement.value;
            } else {
                delete properties.similarity;
            }
        }

        // Date field format
        const dateFormatElement = document.getElementById('dateFormat');
        if (elasticType === 'date' && dateFormatElement && dateFormatElement.value) {
            enhancedProps.format = dateFormatElement.value;
        }

        // Join field relations
        const joinRelationsElement = document.getElementById('joinRelations');
        if (elasticType === 'join' && joinRelationsElement && joinRelationsElement.value.trim()) {
            try {
                enhancedProps.relations = JSON.parse(joinRelationsElement.value);
            } catch (error) {
                showAlert('Invalid JSON in join relations field', 'danger');
                return;
            }
        }

        // Field options
        const indexFieldElement = document.getElementById('indexField');
        const storeFieldElement = document.getElementById('storeField');

        // if (indexFieldElement) enhancedProps.index = indexFieldElement.checked;
        //if (storeFieldElement) enhancedProps.store = storeFieldElement.checked;

        // Merge enhanced properties with custom properties
        const finalProperties = { ...properties, ...enhancedProps };

        // Update mapping field
        const field = mappingFields.find(f => f.field_name === fieldName);
        if (field) {
            field.elastic_type = elasticType;
            field.field_type = elasticType; // Make sure both are set
            field.ui_component_type = uiComponentType;
            field.is_nested = isNested;
            // Always store selected parent even when field isn't marked nested
            field.parent_field = parentFieldv1 || null;
            field.properties = finalProperties;

            // Save dropdown configuration if applicable
            if (uiComponentType === 'dropdown') {
                field.source_index = sourceIndex;
                field.key_field = keyField;
                field.value_field = valueField;
            } else {
                field.source_index = null;
                field.key_field = null;
                field.value_field = null;
            }

            // Save nested fields from the enhanced nested field management
            const nestedFieldsData = [];
            const nestedFieldItems = document.querySelectorAll('#nestedFieldsList .nested-field-item');

            currentConfigField.nested_fields.forEach(item => {
                const nameEl = item.name;
                const typeEl = item.type;

                if (nameEl && typeEl) {
                    const nestedField = {
                        name: nameEl,
                        type: typeEl,
                        properties: {}
                    };

                    // Check if there's stored properties data
                    const propsData = item.properties;
                    if (propsData) {
                        try {
                            nestedField.properties = JSON.parse(propsData);
                        } catch (e) {
                            console.warn('Invalid properties JSON for nested field:', e);
                        }
                    }

                    nestedFieldsData.push(nestedField);
                }
            });

            field.nested_fields = nestedFieldsData;

            // Update section based on field type and parent selection
            if (field.field_type === 'join' || field.parent_field) {
                field.section = 'parent-child';
            } else if (field.field_type === 'nested' || field.field_type === 'object' || field.is_nested) {
                field.section = 'nested';
            } else {
                field.section = 'root';
            }

            console.log('Updated field:', field);

            // Update UI
            updateFieldDisplay(fieldName);
            updateMappingBuilderDisplay();
            updateElasticsearchMappingPreview();
        } else {
            showAlert('Field not found in mapping fields', 'danger');
            return;
        }

        // Close modal
        const modal = bootstrap.Modal.getInstance(document.getElementById('fieldConfigModal'));
        if (modal) {
            modal.hide();
        }

        //showAlert('Field configuration saved successfully', 'success');

    } catch (error) {
        console.error('Error saving field config:', error);
        showAlert('Error saving field configuration: ' + error.message, 'danger');
    }
}

function updateFieldDisplay(fieldName) {
    const fieldElement = document.getElementById(`field_${fieldName}`);
    if (!fieldElement) return;

    const field = mappingFields.find(f => f.field_name === fieldName);
    if (!field) return;

    // Update elastic type display
    const elasticTypeSpan = fieldElement.querySelector('.elastic-type');
    elasticTypeSpan.textContent = field.elastic_type;

    // Update badges
    const configDiv = fieldElement.querySelector('.field-config');
    const nestedBadge = fieldElement.querySelector('.nested-badge');
    const parentBadge = fieldElement.querySelector('.parent-badge');

    if (field.is_nested || field.parent_field) {
        configDiv.style.display = 'block';

        if (field.is_nested) {
            nestedBadge.style.display = 'inline-block';
        } else {
            nestedBadge.style.display = 'none';
        }

        if (field.parent_field) {
            parentBadge.style.display = 'inline-block';
            parentBadge.textContent = `Parent: ${field.parent_field}`;
        } else {
            parentBadge.style.display = 'none';
        }
    } else {
        configDiv.style.display = 'none';
        nestedBadge.style.display = 'none';
        parentBadge.style.display = 'none';
    }

    // Update field styling for parent/child relationships
    if (field.parent_field) {
        fieldElement.classList.add('nested-field');
    } else {
        fieldElement.classList.remove('nested-field');
    }
}

// Mapping generation
async function handleGenerateMapping() {
    const envId = document.getElementById('mappingEnvironment').value;
    const envIdV2 = envId.replace('-', '/');
    const tableName = document.getElementById('mappingTable').value;
    const mappingName = document.getElementById('mappingName').value;

    if (!envId  || !mappingName || mappingFields.length === 0) {
        showAlert('Please fill in all required fields and add at least one mapping field', 'warning');
        return;
    }

    const button = document.getElementById('generateMapping');
    button.disabled = true;
    button.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Generating...';

    try {
        // Prepare enhanced mapping fields with all configurations including UI components
        const enhancedMappingFields = mappingFields.map(field => ({
            field_name: field.field_name,
            oracle_type: field.oracle_type,
            elastic_type: field.elastic_type,
            ui_component_type: field.ui_component_type || 'text_box',
            is_nested: field.is_nested || false,
            parent_field: field.parent_field || null,
            properties: field.properties || {},
            nested_fields: field.nested_fields || [],
            source_index: field.source_index || null,
            key_field: field.key_field || null,
            value_field: field.value_field || null
        }));

        const formData = new FormData();
        formData.append('table_name', tableName);
        formData.append('mapping_name', mappingName);
        formData.append('mapping_fields', JSON.stringify(enhancedMappingFields));

        if (analysisSettings.analyzer && Object.keys(analysisSettings.analyzer).length > 0) {
            formData.append('analysis', JSON.stringify(analysisSettings));
        }

        const usedSimilarities = {};
        mappingFields.forEach(f => {
            if (f.properties && f.properties.similarity) {
                const sim = f.properties.similarity;
                if (similarityDefinitions[sim]) {
                    usedSimilarities[sim] = similarityDefinitions[sim].config;
                }
            }
        });
        if (Object.keys(usedSimilarities).length > 0) {
            formData.append('similarities', JSON.stringify(usedSimilarities));
        }

        const response = await fetch(`/generate-mapping/${envIdV2}`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            throw new Error('Failed to generate mapping');
        }

        const result = await response.json();
        displayGeneratedMapping(result.mapping);
        document.getElementById('mappingPreview').style.display = 'block';

        showAlert('Enhanced mapping generated successfully!', 'success');

    } catch (error) {
        showAlert('Error generating mapping: ' + error.message, 'danger');
    } finally {
        button.disabled = false;
        button.innerHTML = '<i class="fas fa-magic me-2"></i>Generate Mapping';
    }
}

function displayGeneratedMapping(mapping) {
    const container = document.getElementById('mappingJson');
    container.textContent = JSON.stringify(mapping, null, 2);
}

function handleDownloadMapping() {
    const mappingJson = document.getElementById('mappingJson').textContent;
    const mappingName = document.getElementById('mappingName').value;

    const blob = new Blob([mappingJson], { type: 'application/json' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = `${mappingName}_mapping.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);

    URL.revokeObjectURL(url);
}

function showCustomAnalyzerModal() {
    const modal = new bootstrap.Modal(document.getElementById('customAnalyzerModal'));
    document.getElementById('customAnalyzerName').value = '';
    document.getElementById('customTokenizer').value = '';
    document.querySelectorAll('#customTokenFilters input[type="checkbox"]').forEach(cb => cb.checked = false);
    modal.show();
}

function saveCustomAnalyzer() {
    const name = document.getElementById('customAnalyzerName').value.trim();
    const tokenizer = document.getElementById('customTokenizer').value;
    const tokenFilters = Array.from(document.querySelectorAll('#customTokenFilters input[type="checkbox"]:checked')).map(cb => cb.value);

    if (!name || !tokenizer) {
        showAlert('Analyzer name and tokenizer are required', 'warning');
        return;
    }

    const analyzerDef = { tokenizer };
    if (tokenFilters.length) analyzerDef.filter = tokenFilters;
    analysisSettings.analyzer[name] = analyzerDef;

    refreshCustomAnalyzerList();
    populateAnalyzerDropdown();
    bootstrap.Modal.getInstance(document.getElementById('customAnalyzerModal')).hide();
    showAlert('Custom analyzer saved', 'success');
    updateElasticsearchMappingPreview();
}

function refreshCustomAnalyzerList() {
    const listDiv = document.getElementById('customAnalyzerList');
    if (!listDiv) return;
    const names = Object.keys(analysisSettings.analyzer || {});
    if (names.length === 0) {
        listDiv.innerHTML = '<small class="text-muted">No custom analyzers defined</small>';
    } else {
        listDiv.innerHTML = names.map(n => `<span class="badge bg-info me-1">${n}</span>`).join('');
    }
}

function populateAnalyzerDropdown() {
    const select = document.getElementById('analyzer');
    if (!select) return;

    const currentValue = select.value;
    const builtinAnalyzers = [
        'standard',
        'simple',
        'whitespace',
        'keyword',
        'english',
        'light_english',
        'french',
        'german',
        'spanish',
        'chinese',
        'kuromoji',
        'thai',
        'fingerprint',
        'pattern'
    ];

    select.innerHTML = '<option value="">Default</option>';
    builtinAnalyzers.forEach(name => {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name;
        select.appendChild(opt);
    });

    const customNames = Object.keys(analysisSettings.analyzer || {});
    if (customNames.length) {
        const divider = document.createElement('option');
        divider.disabled = true;
        divider.textContent = '— Custom Analyzers —';
        select.appendChild(divider);
        customNames.forEach(name => {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name;
            select.appendChild(opt);
        });
    }

    if (currentValue) {
        select.value = currentValue;
    }
}

function populateSimilarityDropdown() {
    const select = document.getElementById('similarity');
    if (!select) return;

    const currentValue = select.value;
    select.innerHTML = '<option value="">Default</option>';
    Object.entries(similarityDefinitions).forEach(([key, def]) => {
        const opt = document.createElement('option');
        opt.value = key;
        opt.textContent = def.label;
        select.appendChild(opt);
    });

    if (currentValue) {
        select.value = currentValue;
    }
}

// Vector embedding field management
function showVectorEmbeddingModal(fieldName = null) {
    editingVectorField = fieldName;
    const modalEl = document.getElementById('vectorEmbeddingModal');
    const modal = new bootstrap.Modal(modalEl);

    const nameInput = document.getElementById('vectorFieldName');
    const typeSelect = document.getElementById('vectorFieldType');
    const dimsInput = document.getElementById('vectorFieldDims');
    const simSelect = document.getElementById('vectorFieldSimilarity');
    const saveBtn = document.getElementById('saveVectorFieldBtn');

    if (fieldName) {
        const field = mappingFields.find(f => f.field_name === fieldName);
        if (field) {
            nameInput.value = field.field_name;
            let typeValue = 'dense_vector';
            if (field.field_type === 'sparse_vector') {
                typeValue = 'sparse_vector';
            } else if (field.properties.index === true) {
                typeValue = 'dense_vector_index_true';
            } else if (field.properties.index === false) {
                typeValue = 'dense_vector_index_false';
            }
            typeSelect.value = typeValue;
            dimsInput.value = field.properties.dims || '';
            simSelect.value = field.properties.similarity || 'cosine';
            saveBtn.textContent = 'Update';
        }
    } else {
        nameInput.value = '';
        typeSelect.value = 'dense_vector';
        dimsInput.value = '384';
        simSelect.value = 'cosine';
        saveBtn.textContent = 'Add';
    }

    modal.show();
}

function saveVectorField() {
    const name = document.getElementById('vectorFieldName').value.trim();
    const typeVal = document.getElementById('vectorFieldType').value;
    const dims = parseInt(document.getElementById('vectorFieldDims').value, 10);
    const similarity = document.getElementById('vectorFieldSimilarity').value;

    if (!name) {
        showAlert('Field name is required', 'warning');
        return;
    }

    let baseType = 'dense_vector';
    let indexVal;
    if (typeVal === 'dense_vector_index_true') {
        baseType = 'dense_vector';
        indexVal = true;
    } else if (typeVal === 'dense_vector_index_false') {
        baseType = 'dense_vector';
        indexVal = false;
    } else if (typeVal === 'sparse_vector') {
        baseType = 'sparse_vector';
    }

    const fieldObj = {
        field_name: name,
        oracle_type: 'AI_GENERATED',
        elastic_type: baseType,
        field_type: baseType,
        section: 'vector',
        properties: { type: baseType }
    };
    if (!isNaN(dims)) fieldObj.properties.dims = dims;
    if (indexVal !== undefined) fieldObj.properties.index = indexVal;
    if (baseType === 'sparse_vector' || indexVal === true) {
        fieldObj.properties.similarity = similarity;
    }

    if (editingVectorField) {
        const idx = mappingFields.findIndex(f => f.field_name === editingVectorField);
        if (idx !== -1) {
            mappingFields[idx] = fieldObj;
        }
    } else {
        if (mappingFields.some(f => f.field_name === name)) {
            showAlert('Field name already exists', 'warning');
            return;
        }
        mappingFields.push(fieldObj);
    }

    bootstrap.Modal.getInstance(document.getElementById('vectorEmbeddingModal')).hide();
    editingVectorField = null;

    const aiSection = document.getElementById('aiSection');
    if (aiSection) {
        aiSection.style.display = 'block';
        aiSection.classList.add('ai-search-enabled');
    }

    renderVectorEmbeddingFields();
    updateElasticsearchMappingPreview();
}

function renderVectorEmbeddingFields() {
    const container = document.getElementById('aiFieldsContainer');
    if (!container) return;

    container.querySelectorAll('.vector-field-item').forEach(el => el.remove());

    const vectors = mappingFields.filter(f => f.section === 'vector');
    if (vectors.length > 0) {
        Array.from(container.children).forEach(child => {
            if (!child.classList.contains('ai-field-item')) {
                child.remove();
            }
        });
    }

    vectors.forEach(field => {
        const item = document.createElement('div');
        item.className = 'ai-field-item vector-field-item';
        item.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <i class="fas fa-vector-square text-dark me-2"></i>
                    <strong class="text-dark">${field.field_name}</strong>
                </div>
                <div class="btn-group btn-group-sm">
                    <button class="btn btn-outline-secondary" onclick="showVectorEmbeddingModal('${field.field_name}')"><i class="fas fa-edit"></i></button>
                    <button class="btn btn-outline-danger" onclick="deleteVectorField('${field.field_name}')"><i class="fas fa-trash"></i></button>
                </div>
            </div>
            <div class="mt-2">
                <small class="text-muted">${field.field_type}${field.properties.index !== undefined ? ', index: ' + field.properties.index : ''}</small>
            </div>
        `;
        container.appendChild(item);
    });
}

function deleteVectorField(name) {
    if (!confirm(`Are you sure you want to remove field "${name}"?`)) return;
    mappingFields = mappingFields.filter(f => f.field_name !== name);
    renderVectorEmbeddingFields();
    updateElasticsearchMappingPreview();
}

// Saved mappings
async function loadSavedMappings() {
    try {
        const response = await fetch('/mappings');
        const mappings = await response.json();
        displaySavedMappings(mappings);
    } catch (error) {
        showAlert('Error loading saved mappings: ' + error.message, 'danger');
    }
}

function displaySavedMappings(mappings) {
    const container = document.getElementById('savedMappingsList');

    if (mappings.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted">
                <i class="fas fa-folder-open fa-3x mb-3"></i>
                <p>No saved mappings yet. Create one using the Mapping Builder!</p>
            </div>
        `;
        return;
    }

    const table = document.createElement('table');
    table.className = 'table table-striped';
    table.innerHTML = `
        <thead>
            <tr>
                <th>Mapping Name</th>
                <th>Environment</th>
                <th>Table</th>
                <th>Created At</th>
                <th>Actions</th>
            </tr>
        </thead>
        <tbody>
            ${mappings.map(mapping => `
                <tr>
                    <td><strong>${mapping.mapping_name}</strong></td>
                    <td>${mapping.env_name}</td>
                    <td>${mapping.table_name}</td>
                    <td>${new Date(mapping.created_at).toLocaleDateString()}</td>
                    <td>
                        <button class="btn btn-sm btn-outline-primary me-2" onclick="viewMapping(${mapping.id})">
                            <i class="fas fa-eye"></i> View
                        </button>
                        <button class="btn btn-sm btn-outline-success me-2" onclick="downloadSavedMapping(${mapping.id}, '${mapping.mapping_name}')">
                            <i class="fas fa-download"></i> Download
                        </button>
                        <button class="btn btn-sm btn-outline-danger" onclick="confirmDeleteMapping(${mapping.id}, '${mapping.mapping_name}')">
                            <i class="fas fa-trash"></i> Delete
                        </button>
                    </td>
                </tr>
            `).join('')}
        </tbody>
    `;

    container.innerHTML = table.outerHTML;
}

async function viewMapping(mappingId) {
    try {
        const response = await fetch(`/mappings/${mappingId}`);
        const mapping = await response.json();

        const modal = document.createElement('div');
        modal.className = 'modal fade';
        modal.innerHTML = `
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">Mapping: ${mapping.mapping_name}</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="mb-3">
                            <strong>Environment:</strong> ${mapping.env_name}<br>
                            <strong>Table:</strong> ${mapping.table_name}<br>
                            <strong>Created:</strong> ${new Date(mapping.created_at).toLocaleString()}
                        </div>
                        <pre class="mapping-preview">${mapping.mapping_json}</pre>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                        <button type="button" class="btn btn-primary" onclick="downloadMappingFromModal('${mapping.mapping_name}', \`${mapping.mapping_json}\`)">
                            <i class="fas fa-download me-2"></i>Download
                        </button>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(modal);
        const bootstrapModal = new bootstrap.Modal(modal);
        bootstrapModal.show();

        modal.addEventListener('hidden.bs.modal', () => {
            document.body.removeChild(modal);
        });

    } catch (error) {
        showAlert('Error loading mapping: ' + error.message, 'danger');
    }
}

function downloadMappingFromModal(mappingName, mappingJson) {
    const blob = new Blob([mappingJson], { type: 'application/json' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = `${mappingName}_mapping.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);

    URL.revokeObjectURL(url);
}

async function downloadSavedMapping(mappingId, mappingName) {
    try {
        const response = await fetch(`/mappings/${mappingId}`);
        const mapping = await response.json();

        const blob = new Blob([mapping.mapping_json], { type: 'application/json' });
        const url = URL.createObjectURL(blob);

        const a = document.createElement('a');
        a.href = url;
        a.download = `${mappingName}_mapping.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);

        URL.revokeObjectURL(url);

    } catch (error) {
        showAlert('Error downloading mapping: ' + error.message, 'danger');
    }
}

function confirmDeleteMapping(mappingId, mappingName) {
    const modal = document.createElement('div');
    modal.className = 'modal fade';
    modal.innerHTML = `
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">Confirm Delete</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <div class="text-center">
                        <i class="fas fa-exclamation-triangle fa-3x text-warning mb-3"></i>
                        <h6>Are you sure you want to delete this mapping?</h6>
                        <p class="text-muted">
                            <strong>Mapping:</strong> ${mappingName}<br>
                            This action cannot be undone.
                        </p>
                    </div>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                    <button type="button" class="btn btn-danger" onclick="deleteSavedMapping(${mappingId})">
                        <i class="fas fa-trash me-2"></i>Delete Mapping
                    </button>
                </div>
            </div>
        </div>
    `;

    document.body.appendChild(modal);
    const bootstrapModal = new bootstrap.Modal(modal);
    bootstrapModal.show();

    modal.addEventListener('hidden.bs.modal', () => {
        document.body.removeChild(modal);
    });
}

async function deleteSavedMapping(mappingId) {
    try {
        const response = await fetch(`/mappings/${mappingId}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            const errorData = await response.text();
            throw new Error(`Failed to delete mapping (${response.status}): ${errorData}`);
        }

        const result = await response.json();

        // Close the confirmation modal
        const modal = document.querySelector('.modal.show');
        if (modal) {
            const bootstrapModal = bootstrap.Modal.getInstance(modal);
            bootstrapModal.hide();
        }

        // Refresh the saved mappings list
        await loadSavedMappings();

        showAlert('Mapping deleted successfully!', 'success');

    } catch (error) {
        showAlert('Error deleting mapping: ' + error.message, 'danger');
    }
}

// Utility functions
function oracleToElasticType(oracleType) {
    const typeMapping = {
        'VARCHAR2': 'text',
        'CHAR': 'keyword',
        'NUMBER': 'double',
        'DATE': 'date',
        'TIMESTAMP': 'date',
        'CLOB': 'text',
        'BLOB': 'binary',
        'RAW': 'binary',
        'LONG': 'text',
        'INTEGER': 'long',
        'FLOAT': 'double',
        'BINARY_FLOAT': 'float',
        'BINARY_DOUBLE': 'double'
    };
    return typeMapping[oracleType.toUpperCase()] || 'text';
}

function showAlert(message, type = 'info') {
    const alertContainer = document.createElement('div');
    alertContainer.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
    alertContainer.style.cssText = 'top: 20px; right: 20px; z-index: 9999; max-width: 400px;';
    alertContainer.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;

    document.body.appendChild(alertContainer);

    setTimeout(() => {
        if (alertContainer.parentNode) {
            alertContainer.parentNode.removeChild(alertContainer);
        }
    }, 5000);
}

// Enhanced UI Component Functions
function handleUIComponentChange() {
    const uiComponentType = document.getElementById('uiComponentType').value;
    const dropdownConfig = document.getElementById('dropdownConfig');

    if (uiComponentType === 'dropdown') {
        dropdownConfig.style.display = 'block';
        loadAvailableIndices();
    } else {
        dropdownConfig.style.display = 'none';
    }
}

function loadAvailableIndices() {
    const sourceIndex = document.getElementById('sourceIndex');
    // In a real implementation, this would fetch from the current Elasticsearch environment
    // For now, we use the predefined options in the HTML
}

async function loadSourceIndices() {
    const sourceSelect = document.getElementById('sourceIndex');
    sourceSelect.innerHTML = '<option value="">Loading...</option>';

    try {
        const response = await fetch('/api/indices'); // Replace with your actual API URL
        const data = await response.json();

        // Clear and populate the select
        sourceSelect.innerHTML = '<option value="">Select source index...</option>';

        data.forEach(index => {
            const option = document.createElement('option');
            option.value = index.value;
            option.textContent = index.label;
            sourceSelect.appendChild(option);
        });

    } catch (error) {
        console.error('Failed to load indices:', error);
        sourceSelect.innerHTML = '<option value="">Error loading indices</option>';
    }
}

function loadDropdownIndexMapping() {
    const sourceIndex = document.getElementById('sourceIndex').value;
    const keyFieldSelect = document.getElementById('keyField');
    const valueFieldSelect = document.getElementById('valueField');

    // Clear existing options
    keyFieldSelect.innerHTML = '<option value="">Select key field...</option>';
    valueFieldSelect.innerHTML = '<option value="">Select value field...</option>';

    if (!sourceIndex) return;

    // Mock data for demonstration - in real implementation, this would fetch from the selected index
    const mockFields = {
        'categories': ['id', 'name', 'description', 'status'],
        'products': ['id', 'name', 'sku', 'price', 'category_id'],
        'users': ['id', 'username', 'email', 'status', 'created_at'],
        'orders': ['id', 'user_id', 'product_id', 'quantity', 'total']
    };

    const fields = mockFields[sourceIndex] || [];

    fields.forEach(field => {
        const keyOption = document.createElement('option');
        keyOption.value = field;
        keyOption.textContent = field;
        keyFieldSelect.appendChild(keyOption);

        const valueOption = document.createElement('option');
        valueOption.value = field;
        valueOption.textContent = field;
        valueFieldSelect.appendChild(valueOption);
    });
}

// Save to Elasticsearch Functions
function showSaveToElasticsearchModal() {
    const mappingJson = document.getElementById('mappingJson').textContent;

    if (!mappingJson || mappingJson.trim() === 'Add fields to see live mapping preview...') {
        showAlert('Please generate a mapping first before saving to Elasticsearch', 'warning');
        return;
    }

    // Populate Elasticsearch environments
    loadElasticsearchEnvironments();

    // Show mapping preview
    document.getElementById('mappingPreviewForSave').textContent = mappingJson;

    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('saveToElasticsearchModal'));
    modal.show();
}

async function loadElasticsearchEnvironments() {
    try {
        const response = await fetch('/environments');
        const environments = await response.json();
        console.log("Loaded environments:", environments);
        const select = document.getElementById('elasticsearchEnvironment');
        select.innerHTML = '<option value="">Select environment...</option>';

        // ✅ Access the "elasticsearch" array inside the object
        (environments.elasticsearch || []).forEach(env => {
            const option = document.createElement('option');
            option.value = `elasticsearch-${env.id}`; // or just env.id if that's how you use it
            option.textContent = env.name;
            select.appendChild(option);
        });

    } catch (error) {
        console.error('Error loading Elasticsearch environments:', error);
        showAlert('Error loading Elasticsearch environments', 'danger');
    }
}


async function saveToElasticsearch() {
    const envId = document.getElementById('elasticsearchEnvironment').value;
    const indexName = document.getElementById('newIndexName').value;
    const mappingName = document.getElementById('mappingNameForSave').value;
    const mappingJson = document.getElementById('mappingPreviewForSave').textContent; // Must be JSON string

    // Step 1: Validation
    if (!envId || !indexName || !mappingName || !mappingJson) {
        showAlert('Please fill in all required fields', 'warning');
        return;
    }

    // Step 2: Validate index name format
    const indexNameRegex = /^[a-z0-9_-]+$/;
    if (!indexNameRegex.test(indexName)) {
        showAlert('Index name must be lowercase and contain only letters, numbers, hyphens, and underscores', 'warning');
        return;
    }

    // Step 3: Validate mapping JSON
    try {
        JSON.parse(mappingJson);
    } catch (err) {
        showAlert('Mapping JSON is invalid', 'danger');
        return;
    }

    // Step 4: Send form data to FastAPI
    try {
        const formData = new FormData();
        formData.append('index_name', indexName);
        formData.append('mapping_name', mappingName);
        formData.append('mapping_json', mappingJson);
        const fullEnvId = envId.split('-')[1];
        console.log(fullEnvId)
        const response = await fetch(`/save-mapping-to-elasticsearch/${fullEnvId}`, {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (result.success) {
            showAlert(`✅ Mapping saved to Elasticsearch index '${indexName}'`, 'success');

            // Close modal if applicable
            const modalEl = document.getElementById('saveToElasticsearchModal');
            if (modalEl) {
                const modal = bootstrap.Modal.getInstance(modalEl);
                if (modal) modal.hide();
            }

            // Reset form
            document.getElementById('saveToElasticsearchForm').reset();

            // Refresh saved mappings (if you have this function)
            if (typeof loadSavedMappings === 'function') loadSavedMappings();
        } else {
            showAlert('❌ Error saving to Elasticsearch: ' + result.error, 'danger');
        }
    } catch (error) {
        console.error('Error:', error);
        showAlert('❌ Unexpected error: ' + error.message, 'danger');
    }
}


async function saveToElasticsearch_v2() {
    const envId = document.getElementById('elasticsearchEnvironment').value;
    const indexName = document.getElementById('newIndexName').value;
    const mappingName = document.getElementById('mappingNameForSave').value;
    const mappingJson = document.getElementById('mappingJson').value;

    if (!envId || !indexName || !mappingName) {
        showAlert('Please fill in all required fields', 'warning');
        return;
    }

    // Validate index name
    const indexNameRegex = /^[a-z0-9_-]+$/;
    if (!indexNameRegex.test(indexName)) {
        showAlert('Index name must be lowercase and contain only letters, numbers, hyphens, and underscores', 'warning');
        return;
    }

    try {
        const formData = new FormData();
        formData.append('index_name', indexName);
        formData.append('mapping_json', mappingJson);
        formData.append('mapping_name', mappingName);

        const response = await fetch(`/save-mapping-to-elasticsearch/${envId}`, {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (result.success) {
            showAlert(`Mapping saved successfully to Elasticsearch index '${indexName}'`, 'success');

            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('saveToElasticsearchModal'));
            modal.hide();

            // Reset form
            document.getElementById('saveToElasticsearchForm').reset();

            // Refresh saved mappings
            loadSavedMappings();
        } else {
            showAlert('Error saving to Elasticsearch: ' + result.error, 'danger');
        }
    } catch (error) {
        console.error('Error saving to Elasticsearch:', error);
        showAlert('Error saving to Elasticsearch: ' + error.message, 'danger');
    }
}

// Enhanced Nested Field Management Functions
function loadNestedFields(nestedFields) {
    const container = document.getElementById('nestedFieldsList');

    if (!nestedFields || nestedFields.length === 0) {
        container.innerHTML = '<div class="text-muted text-center py-3" id="noNestedFields"><i class="fas fa-info-circle me-2"></i>No nested fields added yet</div>';
        return;
    }

    container.innerHTML = '';

    nestedFields.forEach((nestedField, index) => {
        const fieldElement = createNestedFieldElement(nestedField, index);
        container.appendChild(fieldElement);
    });
}

function createNestedFieldElement(nestedField, index) {
    const div = document.createElement('div');
    div.className = 'nested-field-item p-2 mb-2 border rounded bg-light';
    div.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
            <div>
                <strong>${nestedField.name}</strong>
                <small class="text-muted d-block">${nestedField.type}</small>
                ${nestedField.properties && Object.keys(nestedField.properties).length > 0 ?
        '<span class="badge bg-info">Custom Properties</span>' : ''}
            </div>
            <div>
                <button type="button" class="btn btn-sm btn-outline-primary me-1" onclick="editNestedField(${index})">
                    <i class="fas fa-edit"></i>
                </button>
                <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeNestedField(${index})">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </div>
    `;
    return div;
}

function addQuickNestedField(name, type) {
    if (!currentConfigField) return;

    if (!currentConfigField.nested_fields) {
        currentConfigField.nested_fields = [];
    }

    // Check if field already exists
    const exists = currentConfigField.nested_fields.find(f => f.name === name);
    if (exists) {
        showAlert(`Field "${name}" already exists`, 'warning');
        return;
    }

    const nestedField = {
        name: name,
        type: type,
        properties: {}
    };

    currentConfigField.nested_fields.push(nestedField);
    loadNestedFields(currentConfigField.nested_fields);
    showAlert(`Added "${name}" field`, 'success');
}

function removeNestedField(index) {
    if (!currentConfigField || !currentConfigField.nested_fields) return;

    const fieldName = currentConfigField.nested_fields[index].name;
    currentConfigField.nested_fields.splice(index, 1);
    loadNestedFields(currentConfigField.nested_fields);
    showAlert(`Removed "${fieldName}" field`, 'info');
}

function editNestedField(index) {
    if (!currentConfigField || !currentConfigField.nested_fields) return;

    const nestedField = currentConfigField.nested_fields[index];

    // Populate edit modal
    document.getElementById('nestedFieldName').value = nestedField.name;
    document.getElementById('nestedFieldType').value = nestedField.type;
    document.getElementById('nestedFieldProperties').value = JSON.stringify(nestedField.properties, null, 2);

    // Store current edit index
    window.currentEditIndex = index;

    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('addNestedFieldModal'));
    modal.show();
}

// Event handlers for nested field modal


// Enhanced field options handling
function populateEnhancedFieldOptions(field) {
    const elasticType = field.elastic_type;

    if (elasticType === 'text') {
        populateAnalyzerDropdown();
        populateSimilarityDropdown();
    }

    // Show/hide type-specific options
    document.getElementById('textOptions').style.display = elasticType === 'text' ? 'block' : 'none';
    document.getElementById('dateOptions').style.display = elasticType === 'date' ? 'block' : 'none';
    document.getElementById('joinOptions').style.display = elasticType === 'join' ? 'block' : 'none';

    // Populate field values from properties
    if (field.properties) {
        if (field.properties.analyzer) {
            document.getElementById('analyzer').value = field.properties.analyzer;
        }
        if (field.properties.similarity) {
            document.getElementById('similarity').value = field.properties.similarity;
        }
        if (field.properties.format) {
            document.getElementById('dateFormat').value = field.properties.format;
        }
        if (field.properties.relations) {
            document.getElementById('joinRelations').value = JSON.stringify(field.properties.relations, null, 2);
        }
        // document.getElementById('docValues').checked = field.properties.doc_values !== false;
        // document.getElementById('indexField').checked = field.properties.index !== false;
        //  document.getElementById('storeField').checked = field.properties.store === true;
    } else {
        // Set defaults
        document.getElementById('analyzer').value = '';
        document.getElementById('similarity').value = '';
        document.getElementById('dateFormat').value = '';
        document.getElementById('joinRelations').value = '';
        // document.getElementById('docValues').checked = true;
        // document.getElementById('indexField').checked = true;
        //document.getElementById('storeField').checked = false;
    }
}

// Handle field type changes to show appropriate options
function handleFieldTypeChange() {
    const elasticType = document.getElementById('elasticType').value;

    // Show/hide type-specific options
    document.getElementById('textOptions').style.display = elasticType === 'text' ? 'block' : 'none';
    document.getElementById('dateOptions').style.display = elasticType === 'date' ? 'block' : 'none';
    document.getElementById('joinOptions').style.display = elasticType === 'join' ? 'block' : 'none';

    if (elasticType === 'text') {
        populateAnalyzerDropdown();
        populateSimilarityDropdown();
    }

    // Enable/disable nested fields based on type
    const nestedFieldsSection = document.querySelector('.col-md-6:nth-child(2)');
    if (elasticType === 'nested' || elasticType === 'object') {
        nestedFieldsSection.style.display = 'block';
    } else if (elasticType === 'join') {
        nestedFieldsSection.style.display = 'none';
    }
}

function handleSaveNestedField() {
    console.log('handleSaveNestedField called');

    try {
        const name = document.getElementById('nestedFieldName').value.trim();
        const type = document.getElementById('nestedFieldType').value;
        const propertiesText = document.getElementById('nestedFieldProperties').value.trim();

        if (!name || !type) {
            showAlert('Please fill in field name and type', 'warning');
            return;
        }

        let properties = {};
        try {
            if (propertiesText) {
                properties = JSON.parse(propertiesText);
            }
        } catch (error) {
            showAlert('Invalid JSON in properties field', 'danger');
            return;
        }

        if (!currentConfigField) {
            showAlert('No field is currently being configured', 'warning');
            return;
        }

        if (!currentConfigField.nested_fields) {
            currentConfigField.nested_fields = [];
        }

        const nestedField = {
            name: name,
            type: type,
            properties: properties
        };

        if (window.currentEditIndex !== null && window.currentEditIndex !== undefined) {
            // Editing existing field
            currentConfigField.nested_fields[window.currentEditIndex] = nestedField;
            showAlert(`Updated "${name}" field`, 'success');
        } else {
            // Adding new field
            const exists = currentConfigField.nested_fields.find(f => f.name === name);
            if (exists) {
                showAlert(`Field "${name}" already exists`, 'warning');
                return;
            }

            currentConfigField.nested_fields.push(nestedField);
            showAlert(`Added "${name}" field`, 'success');
        }

        // Update UI
        loadNestedFields(currentConfigField.nested_fields);

        // Clear form and close modal
        document.getElementById('nestedFieldForm').reset();
        const modal = bootstrap.Modal.getInstance(document.getElementById('addNestedFieldModal'));
        if (modal) {
            modal.hide();
        }

    } catch (error) {
        console.error('Error saving nested field:', error);
        showAlert('Error saving nested field: ' + error.message, 'danger');
    }
}
// Update available fields for dragging
function updateAvailableFieldsForDragging() {
    const availableList = document.getElementById('availableFieldsList');
    const availableContainer = document.getElementById('availableFieldsForDrag');

    if (!currentConfigField) {
        if (availableContainer) availableContainer.style.display = 'none';
        return;
    }

    if (!availableContainer || !availableList) {
        console.error('Available fields elements not found');
        return;
    }

    // Only show for nested and object field types
    if (currentConfigField.elastic_type !== 'nested' && currentConfigField.elastic_type !== 'object') {
        availableContainer.style.display = 'none';
        return;
    }

    availableContainer.style.display = 'block';
    availableList.innerHTML = '';

    // Get all fields except current one
    const availableFields = mappingFields.filter(f => f.field_name !== currentConfigField.field_name);

    if (availableFields.length === 0) {
        availableList.innerHTML = '<div class="text-muted text-center p-3">No other fields available</div>';
        return;
    }

    availableFields.forEach(field => {
        const fieldElement = document.createElement('div');
        fieldElement.className = 'draggable-field p-2 mb-2 border rounded bg-light';
        fieldElement.draggable = true;
        fieldElement.dataset.fieldData = JSON.stringify({
            name: field.field_name,
            type: field.elastic_type || field.field_type,
            properties: field.properties || {}
        });

        fieldElement.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <span class="fw-bold">${field.field_name}</span>
                    <span class="badge bg-secondary ms-2">${field.elastic_type || field.field_type}</span>
                </div>
                <i class="fas fa-grip-vertical text-muted"></i>
            </div>
        `;

        // Add drag event listeners
        fieldElement.addEventListener('dragstart', (e) => {
            console.log('Drag start for field:', field.field_name);
            e.dataTransfer.setData('text/plain', fieldElement.dataset.fieldData);
            fieldElement.classList.add('dragging');
        });

        fieldElement.addEventListener('dragend', (e) => {
            fieldElement.classList.remove('dragging');
        });

        availableList.appendChild(fieldElement);
    });

    console.log(`Updated available fields for dragging: ${availableFields.length} fields`);
}

// Handle drag and drop for nested fields
function initializeNestedFieldDragDrop() {
    const dropZone = document.getElementById('nestedFieldsList');

    if (!dropZone) {
        console.error('Nested fields list not found');
        return;
    }

    // Clear existing listeners
    dropZone.removeEventListener('dragover', handleDragOver);
    dropZone.removeEventListener('dragleave', handleDragLeave);
    dropZone.removeEventListener('drop', handleDrop);

    dropZone.addEventListener('dragover', handleDragOver);
    dropZone.addEventListener('dragleave', handleDragLeave);
    dropZone.addEventListener('drop', handleDrop);

    console.log('Initialized nested field drag and drop');
}

function handleDragOver(e) {
    e.preventDefault();
    e.stopPropagation();
    e.currentTarget.classList.add('drag-over');
    console.log('Drag over nested fields list');
}

function handleDragLeave(e) {
    if (!e.currentTarget.contains(e.relatedTarget)) {
        e.currentTarget.classList.remove('drag-over');
    }
}

function handleDrop(e) {
    e.preventDefault();
    e.stopPropagation();
    e.currentTarget.classList.remove('drag-over');

    try {
        const fieldData = JSON.parse(e.dataTransfer.getData('text/plain'));
        console.log('Dropped field data:', fieldData);
        addNestedFieldFromDrop(fieldData);
    } catch (error) {
        console.error('Error handling dropped field:', error);
        showAlert('Error adding dropped field', 'danger');
    }
}

// Add nested field from drag and drop
function addNestedFieldFromDrop(fieldData) {
    if (!currentConfigField || !currentConfigField.nested_fields) {
        return;
    }

    const nestedField = {
        name: fieldData.name,
        type: fieldData.type,
        properties: fieldData.properties || {}
    };

    currentConfigField.nested_fields.push(nestedField);
    loadNestedFields(currentConfigField.nested_fields);
    showAlert('Field added to nested structure', 'success');
}

// ============================================
// ORACLE QUERY RUNNER FUNCTIONALITY
// ============================================

// Oracle Query Runner event handlers
function handleQueryOracleEnvironmentChange() {
    const select = document.getElementById('queryOracleEnvironment');
    const connectBtn = document.getElementById('connectQueryBtn');

    if (select && select.value) {
        connectBtn.disabled = false;
        queryOracleEnvironmentId = parseInt(select.value);
    } else {
        connectBtn.disabled = true;
        queryOracleEnvironmentId = null;
    }
}

async function handleConnectQueryOracle() {
    if (!queryOracleEnvironmentId) return;

    try {
        showLoading('connectQueryBtn');

        // Load tables
        const tablesResponse = await fetch(`/oracle/query-tables/${queryOracleEnvironmentId}`);
        const tablesData = await tablesResponse.json();

        if (tablesData.success) {
            console.log('Oracle tables response:', tablesData);
            queryTables = Array.isArray(tablesData.tables) ? tablesData.tables : [];
            console.log('Query tables set to:', queryTables);
            updateQueryTablesList();
            document.getElementById('queryTablesSection').style.display = 'block';
            document.getElementById('executeQueryBtn').disabled = false;
            showAlert('Connected to Oracle environment successfully', 'success');
        } else {
            throw new Error(tablesData.error || 'Failed to load tables');
        }

    } catch (error) {
        showAlert('Error connecting to Oracle: ' + error.message, 'danger');
    } finally {
        hideLoading('connectQueryBtn');
    }
}

function updateQueryTablesList() {
    const tablesList = document.getElementById('queryTablesList');
    if (!tablesList) {
        console.log('queryTablesList element not found');
        return;
    }

    tablesList.innerHTML = '';

    console.log('Updating query tables list, queryTables:', queryTables);
    console.log('queryTables type:', typeof queryTables);
    console.log('queryTables is array:', Array.isArray(queryTables));

    if (!Array.isArray(queryTables)) {
        console.error('queryTables is not an array:', queryTables);
        tablesList.innerHTML = '<div class="alert alert-warning">No tables data available</div>';
        return;
    }

    if (queryTables.length === 0) {
        tablesList.innerHTML = '<div class="alert alert-info">No tables found in this Oracle environment</div>';
        return;
    }

    queryTables.forEach(table => {
        const item = document.createElement('div');
        item.className = 'list-group-item list-group-item-action d-flex justify-content-between align-items-center';

        // Handle both object format {table_name: "NAME"} and string format "NAME"
        const tableName = typeof table === 'string' ? table : (table.table_name || table.TABLE_NAME || 'Unknown Table');

        item.innerHTML = `
            <div>
                <i class="fas fa-table me-2 text-danger"></i>
                <span>${tableName}</span>
            </div>
            <button class="btn btn-sm btn-outline-primary" onclick="insertTableName('${tableName}')">
                <i class="fas fa-plus"></i>
            </button>
        `;
        tablesList.appendChild(item);
    });
}

function insertTableName(tableName) {
    const queryTextarea = document.getElementById('sqlQuery');
    if (queryTextarea) {
        const currentText = queryTextarea.value;
        const cursorPos = queryTextarea.selectionStart;
        const textBefore = currentText.substring(0, cursorPos);
        const textAfter = currentText.substring(queryTextarea.selectionEnd);

        queryTextarea.value = textBefore + tableName + textAfter;
        queryTextarea.focus();
        queryTextarea.setSelectionRange(cursorPos + tableName.length, cursorPos + tableName.length);
    }
}

async function handleExecuteQuery() {
    const queryTextarea = document.getElementById('sqlQuery');
    const query = queryTextarea.value.trim();

    if (!query) {
        showAlert('Please enter a SQL query', 'warning');
        return;
    }

    if (!queryOracleEnvironmentId) {
        showAlert('Please connect to an Oracle environment first', 'warning');
        return;
    }

    try {
        showLoading('executeQueryBtn');
        hideQueryResults();

        const formData = new FormData();
        formData.append('query', query);

        const response = await fetch(`/oracle/query/${queryOracleEnvironmentId}`, {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (result.success) {
            displayQueryResults(result);
            showAlert(`Query executed successfully. ${result.rowCount} rows returned.`, 'success');
        } else {
            displayQueryError(result.error);
            showAlert('Query execution failed: ' + result.error, 'danger');
        }

    } catch (error) {
        displayQueryError(error.message);
        showAlert('Error executing query: ' + error.message, 'danger');
    } finally {
        hideLoading('executeQueryBtn');
    }
}

// Removed duplicate displayQueryResults function

function displayQueryError(error) {
    const errorDiv = document.getElementById('queryError');
    const resultsDiv = document.getElementById('queryResults');

    if (errorDiv) {
        errorDiv.textContent = error;
        errorDiv.style.display = 'block';
    }
    if (resultsDiv) {
        resultsDiv.style.display = 'none';
    }
}

function displayQueryResults(result) {
    const resultsDiv = document.getElementById('queryResults');
    const tableHead = document.getElementById('resultsTableHead');
    const tableBody = document.getElementById('resultsTableBody');
    const queryInfo = document.getElementById('queryInfo');

    if (!resultsDiv || !tableHead || !tableBody || !queryInfo) {
        console.error('Query results elements not found');
        return;
    }

    // Clear previous results
    tableHead.innerHTML = '';
    tableBody.innerHTML = '';

    if (result.data && result.data.length > 0) {
        // Clear existing content
        tableHead.innerHTML = '';
        tableBody.innerHTML = '';

        // Create table headers
        const headers = result.columns;
        const headerRow = document.createElement('tr');
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header;
            headerRow.appendChild(th);
        });
        tableHead.appendChild(headerRow);

        // Create table rows
        result.data.forEach(row => {
            const tr = document.createElement('tr');
            headers.forEach(header => {
                const td = document.createElement('td');
                td.textContent = row[header] ?? '';  // Use nullish coalescing
                tr.appendChild(td);
            });
            tableBody.appendChild(tr);
        });

        // Update query info
        queryInfo.textContent = `${result.rowCount} rows returned`;

        // Show results
        resultsDiv.style.display = 'block';
    } else {
        // No results
        tableBody.innerHTML = '<tr><td colspan="100%" class="text-center text-muted">No results returned</td></tr>';
        queryInfo.textContent = 'Query executed successfully - no results returned';
        resultsDiv.style.display = 'block';
    }

    // Hide error div
    const queryError = document.getElementById('queryError');
    if (queryError) queryError.style.display = 'none';
}

function hideQueryResults() {
    const queryResults = document.getElementById('queryResults');
    const queryError = document.getElementById('queryError');
    if (queryResults) queryResults.style.display = 'none';
    if (queryError) queryError.style.display = 'none';
}

function handleClearQuery() {
    const queryTextarea = document.getElementById('sqlQuery');
    if (queryTextarea) {
        queryTextarea.value = '';
        queryTextarea.focus();
    }
    hideQueryResults();
}

function handleFormatQuery() {
    const queryTextarea = document.getElementById('sqlQuery');
    if (!queryTextarea) return;

    const query = queryTextarea.value.trim();
    if (!query) {
        showAlert('Please enter a query to format', 'warning');
        return;
    }

    // Basic SQL formatting
    const formatted = query
        .replace(/\s+/g, ' ')  // Replace multiple spaces
        .replace(/\(\s+/g, '(')  // Remove space after (
        .replace(/\s+\)/g, ')')  // Remove space before )
        .replace(/,\s*/g, ',\n    ')  // Line break after commas
        .replace(/\bSELECT\b/gi, 'SELECT')
        .replace(/\bFROM\b/gi, '\nFROM')
        .replace(/\bWHERE\b/gi, '\nWHERE')
        .replace(/\bAND\b/gi, '\n  AND')
        .replace(/\bOR\b/gi, '\n   OR')
        .replace(/\bORDER BY\b/gi, '\nORDER BY')
        .replace(/\bGROUP BY\b/gi, '\nGROUP BY')
        .replace(/\bHAVING\b/gi, '\nHAVING')
        .replace(/\bINNER JOIN\b/gi, '\nINNER JOIN')
        .replace(/\bLEFT JOIN\b/gi, '\nLEFT JOIN')
        .replace(/\bRIGHT JOIN\b/gi, '\nRIGHT JOIN')
        .replace(/\bFULL JOIN\b/gi, '\nFULL JOIN');

    queryTextarea.value = formatted;
    showAlert('Query formatted successfully', 'success');
}

// ============================================
// ORACLE MAPPING BUILDER FUNCTIONALITY
// ============================================

function handleMappingOracleEnvironmentChange() {
    const select = document.getElementById('mappingOracleEnvironment');
    const connectBtn = document.getElementById('connectMappingBtn');

    if (select && select.value) {
        connectBtn.disabled = false;
        mappingOracleEnvironmentId = parseInt(select.value);
    } else {
        connectBtn.disabled = true;
        mappingOracleEnvironmentId = null;
    }

    // Reset table selection
    resetMappingTableSelection();
}

function resetMappingTableSelection() {
    const mappingTablesSection = document.getElementById('mappingTablesSection');
    const oracleTableColumnsSection = document.getElementById('oracleTableColumnsSection');
    const oracleNoTableSelected = document.getElementById('oracleNoTableSelected');

    if (mappingTablesSection) mappingTablesSection.style.display = 'none';
    if (oracleTableColumnsSection) oracleTableColumnsSection.style.display = 'none';
    if (oracleNoTableSelected) oracleNoTableSelected.style.display = 'block';

    selectedOracleTable = null;
    oracleTableColumns = [];
    selectedOracleColumns = [];
}

async function handleConnectMappingOracle() {
    if (!mappingOracleEnvironmentId) return;

    try {
        showLoading('connectMappingBtn');

        // Load tables
        const tablesResponse = await fetch(`/oracle/mapping-tables/${mappingOracleEnvironmentId}`);
        const tablesData = await tablesResponse.json();

        if (tablesData.success) {
            mappingTables = tablesData.tables;
            updateMappingTableSelect();
            document.getElementById('mappingTablesSection').style.display = 'block';
            showAlert('Connected to Oracle environment successfully', 'success');
        } else {
            throw new Error(tablesData.error || 'Failed to load tables');
        }

    } catch (error) {
        showAlert('Error connecting to Oracle: ' + error.message, 'danger');
    } finally {
        hideLoading('connectMappingBtn');
    }
}

function updateMappingTableSelect() {
    const select = document.getElementById('mappingTableSelect');
    if (!select) return;

    select.innerHTML = '<option value="">Select Table...</option>';

    mappingTables.forEach(table => {
        const option = document.createElement('option');
        option.value = table.table_name;
        option.textContent = table.table_name;
        select.appendChild(option);
    });
}

function handleMappingTableChange() {
    const select = document.getElementById('mappingTableSelect');
    const loadBtn = document.getElementById('loadTableStructureBtn');
    const tableSelect = document.getElementById('mappingTable');
    const loadColumnsBtn = document.getElementById('loadColumns');

    if (tableSelect && loadColumnsBtn) {
        const tableName = tableSelect.value;
        loadColumnsBtn.disabled = !tableName;  // ← This enables/disables Load Fields button
    }

    if (select && select.value) {
        selectedOracleTable = select.value;
        loadBtn.disabled = false;
        loadBtn.style.display = 'block';
        showAlert(`Selected table: ${selectedOracleTable}`, 'info');
    } else {
        selectedOracleTable = null;
        loadBtn.disabled = true;
        loadBtn.style.display = 'none';

        // Hide table columns section
        const oracleTableColumnsSection = document.getElementById('oracleTableColumnsSection');
        const oracleNoTableSelected = document.getElementById('oracleNoTableSelected');
        if (oracleTableColumnsSection) oracleTableColumnsSection.style.display = 'none';
        if (oracleNoTableSelected) oracleNoTableSelected.style.display = 'block';

        // Reset columns data
        oracleTableColumns = [];
        selectedOracleColumns = [];
    }
}

async function handleLoadTableStructure() {
    if (!selectedOracleTable || !mappingOracleEnvironmentId) return;

    try {
        showLoading('loadTableStructureBtn');

        const response = await fetch(`/oracle/table-structure/${mappingOracleEnvironmentId}/${selectedOracleTable}`);
        const result = await response.json();

        if (result.success) {
            console.log('Oracle table structure response:', result);
            oracleTableColumns = Array.isArray(result.columns) ? result.columns : [];
            console.log('Oracle table columns set to:', oracleTableColumns);
            selectedOracleColumns = [];
            updateOracleColumnsList();
            document.getElementById('oracleTableColumnsSection').style.display = 'block';
            document.getElementById('oracleNoTableSelected').style.display = 'none';
            showAlert(`Loaded ${oracleTableColumns.length} columns from ${selectedOracleTable}`, 'success');
        } else {
            throw new Error(result.error || 'Failed to load table structure');
        }

    } catch (error) {
        showAlert('Error loading table structure: ' + error.message, 'danger');
    } finally {
        hideLoading('loadTableStructureBtn');
    }
}

function updateOracleColumnsList() {
    const columnsList = document.getElementById('oracleColumnsList');
    if (!columnsList) {
        console.log('oracleColumnsList element not found');
        return;
    }

    columnsList.innerHTML = '';

    console.log('Updating Oracle columns list, oracleTableColumns:', oracleTableColumns);

    if (!Array.isArray(oracleTableColumns)) {
        console.error('oracleTableColumns is not an array:', oracleTableColumns);
        columnsList.innerHTML = '<div class="alert alert-warning">No columns data available</div>';
        return;
    }

    if (oracleTableColumns.length === 0) {
        columnsList.innerHTML = '<div class="alert alert-info">No columns found in this table</div>';
        return;
    }

    oracleTableColumns.forEach((column, index) => {
        const columnDiv = document.createElement('div');
        columnDiv.className = 'column-entry bg-light p-2 mb-2 rounded';

        const columnName = column.name || column.column_name || 'Unknown';
        const columnType = column.type || column.data_type || 'Unknown Type';
        const elasticType = getElasticsearchType(columnType);

        columnDiv.innerHTML = `
            <div class="form-check">
                <input class="form-check-input oracle-column-checkbox" type="checkbox" 
                       value="${columnName}" id="oracleCol_${index}">
                <label class="form-check-label w-100" for="oracleCol_${index}">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <strong>${columnName}</strong>
                            <br>
                            <small class="text-muted">${columnType}</small>
                        </div>
                        <span class="badge bg-secondary">${elasticType}</span>
                    </div>
                </label>
            </div>
        `;

        // Add event listener to checkbox
        const checkbox = columnDiv.querySelector('.oracle-column-checkbox');
        checkbox.addEventListener('change', handleOracleColumnChange);

        columnsList.appendChild(columnDiv);
    });

    // Update Oracle mapping preview immediately
    updateOracleMappingPreview();
}

function getElasticsearchType(oracleType) {
    const typeMap = {
        'VARCHAR2': 'text',
        'CHAR': 'keyword',
        'NUMBER': 'long',
        'DATE': 'date',
        'TIMESTAMP': 'date',
        'CLOB': 'text',
        'BLOB': 'binary',
        'INTEGER': 'long',
        'FLOAT': 'double'
    };
    return typeMap[oracleType.toUpperCase()] || 'text';
}

function handleOracleColumnChange(event) {
    const checkbox = event.target;
    const columnName = checkbox.value;

    console.log(`Oracle column change: ${columnName}, checked: ${checkbox.checked}`);

    if (checkbox.checked) {
        if (!selectedOracleColumns.includes(columnName)) {
            selectedOracleColumns.push(columnName);
        }
    } else {
        selectedOracleColumns = selectedOracleColumns.filter(col => col !== columnName);
    }

    console.log('Selected Oracle columns:', selectedOracleColumns);
    updateOracleMappingPreview();
    updateOracleMappingButtons();
}

function selectAllOracleColumns() {
    const checkboxes = document.querySelectorAll('.oracle-column-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = true;
        const columnName = checkbox.value;
        if (!selectedOracleColumns.includes(columnName)) {
            selectedOracleColumns.push(columnName);
        }
    });
    updateOracleMappingPreview();
    updateOracleMappingButtons();
}

function clearAllOracleColumns() {
    const checkboxes = document.querySelectorAll('.oracle-column-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });
    selectedOracleColumns = [];
    updateOracleMappingPreview();
    updateOracleMappingButtons();
}

function updateOracleMappingPreview() {
    const preview = document.getElementById('oracleMappingPreview');
    const fieldCountBadge = document.getElementById('oracleFieldCount');
    const mappingSizeBadge = document.getElementById('oracleMappingSize');

    if (!preview) return;

    if (selectedOracleColumns.length === 0) {
        preview.textContent = 'Select Oracle columns to generate mapping preview...';
        if (fieldCountBadge) fieldCountBadge.textContent = '0 columns';
        if (mappingSizeBadge) mappingSizeBadge.textContent = '0 KB';
        return;
    }

    const mapping = {
        "mappings": {
            "properties": {}
        }
    };

    selectedOracleColumns.forEach(columnName => {
        // Handle both property name formats
        const column = oracleTableColumns.find(col =>
            (col.name && col.name === columnName) ||
            (col.column_name && col.column_name === columnName)
        );

        if (column) {
            const oracleType = column.type || column.data_type || 'VARCHAR2';
            const elasticType = getElasticsearchType(oracleType);
            const fieldName = columnName.toLowerCase();

            mapping.mappings.properties[fieldName] = {
                "type": elasticType
            };

            // Add keyword subfield for text fields
            if (elasticType === 'text') {
                mapping.mappings.properties[fieldName].fields = {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                };
            }

            // Add format for date fields
            if (elasticType === 'date') {
                mapping.mappings.properties[fieldName].format = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis";
            }
        }
    });

    const mappingJson = JSON.stringify(mapping, null, 2);
    preview.textContent = mappingJson;

    // Update badges
    if (fieldCountBadge) {
        fieldCountBadge.textContent = `${selectedOracleColumns.length} column${selectedOracleColumns.length !== 1 ? 's' : ''}`;
    }
    if (mappingSizeBadge) {
        const sizeKB = Math.ceil(mappingJson.length / 1024);
        mappingSizeBadge.textContent = `${sizeKB} KB`;
    }

    // Update Oracle visual display
    updateOracleVisualDisplay();
}

function updateOracleVisualDisplay() {
    const visualDisplay = document.getElementById('oracleVisualDisplay');
    if (!visualDisplay) return;

    if (selectedOracleColumns.length === 0) {
        visualDisplay.innerHTML = '<div class="text-center text-muted p-4"><i class="fas fa-database fa-3x mb-3 text-danger"></i><p>Select Oracle columns to see visual mapping structure</p></div>';
        return;
    }

    let visualHtml = '<div class="field-tree">';

    // Add Oracle table root
    const tableName = selectedOracleTable || 'Oracle Table';
    visualHtml += `
        <div class="field-tree-item field-tree-root" style="background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);">
            <div class="field-tree-icon">
                <i class="fas fa-database"></i>
            </div>
            <div class="field-tree-name">${tableName}</div>
            <div class="field-tree-type bg-light text-dark">${selectedOracleColumns.length} columns → Elasticsearch</div>
        </div>
    `;

    // Add Oracle columns with their Elasticsearch mappings
    selectedOracleColumns.forEach(columnName => {
        // Handle both property name formats
        const column = oracleTableColumns.find(col =>
            (col.name && col.name === columnName) ||
            (col.column_name && col.column_name === columnName)
        );

        if (column) {
            const oracleType = column.type || column.data_type || 'VARCHAR2';
            const elasticType = getElasticsearchType(oracleType);
            const typeIcon = getFieldTypeIcon(elasticType);

            visualHtml += `
                <div class="field-tree-item field-type-${elasticType}">
                    <div class="field-tree-icon">
                        ${typeIcon}
                    </div>
                    <div class="field-tree-name">${columnName.toLowerCase()}</div>
                    <div class="field-tree-type bg-primary text-white">${elasticType}</div>
                </div>
            `;

            // Show Oracle type info
            visualHtml += `
                <div class="nested-field-container">
                    <div class="field-tree-item" style="background: #fff5f5; border-color: #dc3545;">
                        <div class="field-tree-icon" style="background: #dc3545; color: white;">
                            <i class="fas fa-database"></i>
                        </div>
                        <div class="field-tree-name">Oracle: ${oracleType}</div>
                        <div class="field-tree-type bg-danger text-white">Source</div>
                    </div>
                </div>
            `;

            // Show keyword subfield for text fields
            if (elasticType === 'text') {
                visualHtml += `
                    <div class="nested-field-container">
                        <div class="field-tree-item field-type-keyword">
                            <div class="field-tree-icon">
                                <i class="fas fa-key"></i>
                            </div>
                            <div class="field-tree-name">${columnName.toLowerCase()}.keyword</div>
                            <div class="field-tree-type bg-secondary text-white">keyword</div>
                        </div>
                    </div>
                `;
            }
        }
    });

    visualHtml += '</div>';
    visualDisplay.innerHTML = visualHtml;
}

function getFieldTypeIcon(fieldType) {
    const iconMap = {
        'text': '<i class="fas fa-font"></i>',
        'keyword': '<i class="fas fa-key"></i>',
        'long': '<i class="fas fa-sort-numeric-up"></i>',
        'integer': '<i class="fas fa-hashtag"></i>',
        'double': '<i class="fas fa-calculator"></i>',
        'float': '<i class="fas fa-percentage"></i>',
        'date': '<i class="fas fa-calendar"></i>',
        'boolean': '<i class="fas fa-toggle-on"></i>',
        'binary': '<i class="fas fa-file-code"></i>',
        'nested': '<i class="fas fa-layer-group"></i>',
        'object': '<i class="fas fa-cube"></i>',
        'join': '<i class="fas fa-link"></i>',
        'dense_vector': '<i class="fas fa-vector-square"></i>'
    };
    return iconMap[fieldType] || '<i class="fas fa-question"></i>';
}

function updateOracleMappingButtons() {
    const generateBtn = document.getElementById('generateOracleBtn');
    const mappingNameInput = document.getElementById('oracleMappingName');

    const mappingName = mappingNameInput ? mappingNameInput.value.trim() : '';
    const canGenerate = selectedOracleColumns.length > 0 && mappingName;

    if (generateBtn) generateBtn.disabled = !canGenerate;
}

async function handleGenerateOracleMapping() {
    const mappingNameInput = document.getElementById('oracleMappingName');
    const mappingName = mappingNameInput ? mappingNameInput.value.trim() : '';

    if (!mappingName) {
        showAlert('Please enter a mapping name', 'warning');
        return;
    }

    if (selectedOracleColumns.length === 0) {
        showAlert('Please select at least one column', 'warning');
        return;
    }

    try {
        showLoading('generateOracleBtn');

        const formData = new FormData();
        formData.append('table_name', selectedOracleTable);
        formData.append('mapping_name', mappingName);
        formData.append('selected_columns', JSON.stringify(selectedOracleColumns));

        const response = await fetch(`/oracle/generate-table-mapping/${mappingOracleEnvironmentId}`, {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (result.success) {
            showAlert(`Mapping "${mappingName}" generated and saved successfully!`, 'success');
            updateOracleMappingPreview();

            // Clear form
            if (mappingNameInput) mappingNameInput.value = '';
            clearAllOracleColumns();
        } else {
            throw new Error(result.error || 'Failed to generate mapping');
        }

    } catch (error) {
        showAlert('Error generating mapping: ' + error.message, 'danger');
    } finally {
        hideLoading('generateOracleBtn');
    }
}

// Enhanced Preview Functions
function updateElasticsearchMappingPreview() {
    const preview = document.getElementById('mappingJson');
    const fieldSummary = document.getElementById('fieldSummary');
    const validationStatus = document.getElementById('validationStatus');
    const fieldCountBadge = document.getElementById('fieldCount');
    const mappingSizeBadge = document.getElementById('mappingSize');
    const visualDisplay = document.getElementById('visualMappingDisplay');

    if (!preview) return;

    if (mappingFields.length === 0) {
        preview.textContent = 'Add fields to see live mapping preview...';
        if (fieldSummary) {
            fieldSummary.innerHTML = '<div class="text-center text-muted"><i class="fas fa-info-circle me-2"></i>No fields added yet</div>';
        }
        if (validationStatus) {
            validationStatus.innerHTML = '<div class="text-center text-muted"><i class="fas fa-check-circle text-success me-2"></i>Ready to build</div>';
        }
        if (fieldCountBadge) fieldCountBadge.textContent = '0 fields';
        if (mappingSizeBadge) mappingSizeBadge.textContent = '0 KB';
        if (visualDisplay) {
            visualDisplay.innerHTML = '<div class="text-center text-muted p-4"><i class="fas fa-sitemap fa-3x mb-3"></i><p>Add fields to see visual mapping structure</p></div>';
        }
        updateRelationSummary();
        updateMappingStats();
        return;
    }

    // Generate mapping
    const mapping = {
        "mappings": {
            "properties": {}
        }
    };

    let settings = {};
    if (analysisSettings.analyzer && Object.keys(analysisSettings.analyzer).length > 0) {
        settings.analysis = { analyzer: analysisSettings.analyzer };
    }

    const usedSimilarities = {};

    // First build field definitions map
    const fieldDefinitions = {};
    mappingFields.forEach(field => {
        const fieldDef = { type: field.field_type };

        if (field.properties && Object.keys(field.properties).length > 0) {
            Object.assign(fieldDef, field.properties);
        }

        if (field.nested_fields && field.nested_fields.length > 0 &&
            (field.field_type === 'nested' || field.field_type === 'object')) {
            fieldDef.properties = {};
            field.nested_fields.forEach(nestedField => {
                fieldDef.properties[nestedField.name] = {
                    type: nestedField.type || 'text'
                };
                if (nestedField.properties) {
                    Object.assign(fieldDef.properties[nestedField.name], nestedField.properties);
                }
            });
        }

        if (field.field_type === 'text') {
            fieldDef.fields = {
                keyword: {
                    type: 'keyword',
                    ignore_above: 256
                }
            };
        }

        if (field.properties && field.properties.similarity) {
            const simName = field.properties.similarity;
            if (similarityDefinitions[simName]) {
                usedSimilarities[simName] = similarityDefinitions[simName].config;
            }
        }

        fieldDefinitions[field.field_name] = fieldDef;
    });

    // Then assemble hierarchy, nesting fields under their parents
    const rootProperties = {};
    mappingFields.forEach(field => {
        const fieldDef = fieldDefinitions[field.field_name];
        if (field.parent_field && fieldDefinitions[field.parent_field]) {
            const parentDef = fieldDefinitions[field.parent_field];
            if (!parentDef.properties) parentDef.properties = {};
            parentDef.properties[field.field_name] = fieldDef;
        } else {
            rootProperties[field.field_name] = fieldDef;
        }
    });

    mapping.mappings.properties = rootProperties;

    if (Object.keys(usedSimilarities).length > 0) {
        settings.similarity = usedSimilarities;
    }
    if (Object.keys(settings).length > 0) {
        mapping.settings = settings;
    }

    const mappingJson = JSON.stringify(mapping, null, 2);
    preview.textContent = mappingJson;

    // Update field summary
    if (fieldSummary) {
        let summaryHtml = '';
        mappingFields.forEach(field => {
            summaryHtml += `
                <div class="field-summary-item">
                    <span>${field.field_name}</span>
                    <span class="badge bg-primary field-type-badge">${field.oracle_type}</span>
                </div>
            `;
        });
        fieldSummary.innerHTML = summaryHtml;
    }

    // Update validation status
    if (validationStatus) {
        const hasNested = mappingFields.some(f => f.field_type === 'nested' || f.field_type === 'object');
        const hasJoin = mappingFields.some(f => f.field_type === 'join');

        let validationHtml = '<div class="validation-item"><i class="fas fa-check-circle text-success"></i>Valid mapping structure</div>';
        if (hasNested) {
            validationHtml += '<div class="validation-item"><i class="fas fa-info-circle text-info"></i>Contains nested fields</div>';
        }
        if (hasJoin) {
            validationHtml += '<div class="validation-item"><i class="fas fa-link text-warning"></i>Contains join fields</div>';
        }

        validationStatus.innerHTML = validationHtml;
    }

    // Update badges
    if (fieldCountBadge) {
        fieldCountBadge.textContent = `${mappingFields.length} field${mappingFields.length !== 1 ? 's' : ''}`;
    }
    if (mappingSizeBadge) {
        const sizeKB = Math.ceil(mappingJson.length / 1024);
        mappingSizeBadge.textContent = `${sizeKB} KB`;
    }

    // Update visual representation
    updateVisualMappingDisplay();
    updateRelationSummary();
    updateMappingStats();
}

function updateVisualMappingDisplay() {
    const visualDisplay = document.getElementById('visualMappingDisplay');
    if (!visualDisplay) return;

    if (mappingFields.length === 0) {
        visualDisplay.innerHTML = '<div class="text-center text-muted p-4"><i class="fas fa-sitemap fa-3x mb-3"></i><p>Add fields to see visual mapping structure</p></div>';
        return;
    }

    let visualHtml = '<div class="field-tree">';

    // Add root mapping container
    visualHtml += `
        <div class="field-tree-item field-tree-root">
            <div class="field-tree-icon">
                <i class="fas fa-database"></i>
            </div>
            <div class="field-tree-name">Elasticsearch Index Mapping</div>
            <div class="field-tree-type bg-light text-dark">${mappingFields.length} fields</div>
        </div>
    `;

    // Add each field
    mappingFields.forEach(field => {
        const typeIcon = getFieldTypeIcon(field.elastic_type);
        visualHtml += `
            <div class="field-tree-item field-type-${field.elastic_type}">
                <div class="field-tree-icon">
                    ${typeIcon}
                </div>
                <div class="field-tree-name">${field.field_name}</div>
                <div class="field-tree-type bg-primary text-white">${field.oracle_type}</div>
            </div>
        `;

        // Add nested fields if any
        if (field.nested_fields && field.nested_fields.length > 0) {
            visualHtml += '<div class="nested-field-container">';
            field.nested_fields.forEach(nestedField => {
                const nestedIcon = getFieldTypeIcon(nestedField.type || 'text');
                visualHtml += `
                    <div class="field-tree-item field-type-${nestedField.type || 'text'}">
                        <div class="field-tree-icon">
                            ${nestedIcon}
                        </div>
                        <div class="field-tree-name">${nestedField.name}</div>
                        <div class="field-tree-type bg-secondary text-white">${nestedField.type || 'text'}</div>
                    </div>
                `;
            });
            visualHtml += '</div>';
        }

        // Show keyword subfield for text fields
        if (field.field_type === 'text') {
            visualHtml += `
                <div class="nested-field-container">
                    <div class="field-tree-item field-type-keyword">
                        <div class="field-tree-icon">
                            <i class="fas fa-key"></i>
                        </div>
                        <div class="field-tree-name">${field.field_name}.keyword</div>
                        <div class="field-tree-type bg-secondary text-white">keyword</div>
                    </div>
                </div>
            `;
        }
    });

    visualHtml += '</div>';
    visualDisplay.innerHTML = visualHtml;
}



function updateMappingStats() {
    const totalFieldsEl = document.getElementById('totalFields');
    const nestedFieldsEl = document.getElementById('nestedFields');
    const textFieldsEl = document.getElementById('textFields');
    const dateFieldsEl = document.getElementById('dateFields');
    const estimatedSizeEl = document.getElementById('estimatedSize');

    if (!totalFieldsEl) return;

    const totalFields = mappingFields.length;
    const nestedFields = mappingFields.filter(f => f.field_type === 'nested' || f.field_type === 'object').length;
    const textFields = mappingFields.filter(f => f.field_type === 'text').length;
    const dateFields = mappingFields.filter(f => f.field_type === 'date').length;

    // Calculate estimated size
    let estimatedSize = 0;
    mappingFields.forEach(field => {
        estimatedSize += JSON.stringify(field).length;
        if (field.nested_fields) {
            estimatedSize += JSON.stringify(field.nested_fields).length;
        }
    });
    const sizeKB = Math.ceil(estimatedSize / 1024);

    totalFieldsEl.textContent = totalFields;
    nestedFieldsEl.textContent = nestedFields;
    textFieldsEl.textContent = textFields;
    dateFieldsEl.textContent = dateFields;
    estimatedSizeEl.textContent = `${sizeKB} KB`;
}

function updateRelationSummary() {
    const relationSummaryEl = document.getElementById('relationSummary');
    if (!relationSummaryEl) return;

    const relationFields = mappingFields.filter(f => f.section === 'nested' || f.section === 'parent-child');

    if (relationFields.length === 0) {
        relationSummaryEl.innerHTML = '<div class="text-center text-muted"><i class="fas fa-sitemap me-2"></i>No nested or parent-child fields</div>';
        return;
    }

    relationSummaryEl.innerHTML = relationFields.map(f => {
        if (f.section === 'nested') {
            return `<div class="d-flex justify-content-between mb-1"><span>${f.field_name}</span><span class="badge bg-success ms-2">nested</span></div>`;
        }
        let relText = 'parent-child';
        if (f.properties && f.properties.relations) {
            const relations = f.properties.relations;
            const parent = Object.keys(relations)[0];
            const child = relations[parent];
            relText = `${parent} → ${child}`;
        }
        return `<div class="d-flex justify-content-between mb-1"><span>${f.field_name}</span><span class="badge bg-primary ms-2">${relText}</span></div>`;
    }).join('');
}

// Enhanced Field Management Functions

// Add field by type with quick configuration
function addFieldByType(fieldType) {
    const fieldName = prompt(`Enter name for ${fieldType} field:`);
    if (!fieldName) return;

    const field = {
        field_name: fieldName,
        field_type: fieldType,
        is_nested: false,
        parent_field: null,
        properties: {},
        nested_fields: [],
        section: 'root'
    };

    // Add default properties based on field type
    switch (fieldType) {
        case 'text':
            field.properties = { "index": true, "analyzer": "standard" };
            break;
        case 'date':
            field.properties = { "format": "yyyy-MM-dd||yyyy-MM-dd HH:mm:ss||epoch_millis" };
            break;
        case 'keyword':
            field.properties = { "ignore_above": 256 };
            break;
        case 'long':
        case 'integer':
        case 'double':
        case 'float':
            field.properties = { "index": true };
            break;
    }

    mappingFields.push(field);
    updateMappingBuilderDisplay();
    updateElasticsearchMappingPreview();
    showAlert(`${fieldType} field "${fieldName}" added successfully`, 'success');
}

// Add nested field
function addNestedField() {
    const fieldName = prompt('Enter name for nested field:');
    if (!fieldName) return;

    const field = {
        field_name: fieldName,
        field_type: 'nested',
        is_nested: true,
        parent_field: null,
        properties: { "type": "nested" },
        nested_fields: [],
        section: 'nested'
    };

    mappingFields.push(field);
    updateMappingBuilderDisplay();
    updateElasticsearchMappingPreview();
    showAlert(`Nested field "${fieldName}" added successfully`, 'success');

    // Open configuration modal for adding sub-fields
    setTimeout(() => {
        configureField(fieldName);
    }, 500);
}

// Add parent-child relationship
function addParentChildField() {
    // Check if modal exists
    const modalElement = document.getElementById('parentChildModal');
    if (!modalElement) {
        showAlert('Parent-child configuration modal not found. Please check the HTML template.', 'warning');
        return;
    }

    const modal = new bootstrap.Modal(modalElement);
    modal.show();
}

// Save parent-child relationship
function saveParentChildRelation() {
    const relationshipName = document.getElementById('relationshipName').value;
    const parentType = document.getElementById('parentType').value;
    const childType = document.getElementById('childType').value;

    if (!relationshipName || !parentType || !childType) {
        showAlert('Please fill all parent-child relationship fields', 'error');
        return;
    }

    const field = {
        field_name: relationshipName,
        field_type: 'join',
        is_nested: false,
        parent_field: null,
        properties: {
            "type": "join",
            "relations": {
                [parentType]: childType
            }
        },
        nested_fields: [],
        section: 'parent-child'
    };

    mappingFields.push(field);
    updateMappingBuilderDisplay();
    updateElasticsearchMappingPreview();

    // Clear form and close modal
    document.getElementById('relationshipName').value = '';
    document.getElementById('parentType').value = '';
    document.getElementById('childType').value = '';
    bootstrap.Modal.getInstance(document.getElementById('parentChildModal')).hide();

    showAlert(`Parent-child relationship "${relationshipName}" created successfully`, 'success');
}

// Update mapping builder display with sections
function updateMappingBuilderDisplay() {
    const rootSection = document.getElementById('rootFieldsSection');
    const nestedSection = document.getElementById('nestedFieldsSection');
    const parentChildSection = document.getElementById('parentChildSection');

    // Clear sections
    [rootSection, nestedSection, parentChildSection].forEach(section => {
        if (section) {
            section.innerHTML = '';
        }
    });

    // Populate sections
    mappingFields.forEach(field => {
        const fieldElement = createFieldElement(field);

        switch (field.section || 'root') {
            case 'root':
                if (rootSection) rootSection.appendChild(fieldElement);
                break;
            case 'nested':
                if (nestedSection) nestedSection.appendChild(fieldElement);
                break;
            case 'parent-child':
                if (parentChildSection) parentChildSection.appendChild(fieldElement);
                break;
        }
    });

    // Show empty state messages if sections are empty
    if (rootSection && rootSection.children.length === 0) {
        rootSection.innerHTML = '<div class="text-center text-muted"><i class="fas fa-plus fa-2x mb-2"></i><p>Add root-level fields here</p></div>';
    }
    if (nestedSection && nestedSection.children.length === 0) {
        nestedSection.innerHTML = '<div class="text-center text-muted"><i class="fas fa-layer-group fa-2x mb-2"></i><p>Drag nested fields here</p></div>';
    }
    if (parentChildSection && parentChildSection.children.length === 0) {
        parentChildSection.innerHTML = '<div class="text-center text-muted"><i class="fas fa-link fa-2x mb-2"></i><p>Set up parent-child relationships here</p></div>';
    }

    renderVectorEmbeddingFields();
}

// Create field element for display
function createFieldElement(field) {
    const fieldDiv = document.createElement('div');
    fieldDiv.className = 'field-item';
    fieldDiv.draggable = true;
    fieldDiv.dataset.fieldName = field.field_name;

    const typeIcon = getFieldTypeIcon(field.field_type);
    const typeBadge = getFieldTypeBadge(field.field_type);

    fieldDiv.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
            <div class="d-flex align-items-center">
                <span class="drag-handle me-2">
                    <i class="fas fa-grip-vertical"></i>
                </span>
                <div class="field-tree-icon field-type-${field.field_type} me-2">
                    ${typeIcon}
                </div>
                <div>
                    <strong>${field.field_name}</strong>
                    ${typeBadge}
                    ${field.nested_fields && field.nested_fields.length > 0 ? `<br><small class="text-muted">${field.nested_fields.length} sub-fields</small>` : ''}
                </div>
            </div>
            <div class="btn-group" role="group">
                <button type="button" class="btn btn-sm btn-outline-primary" onclick="configureField('${field.field_name}')" title="Configure">
                    <i class="fas fa-cog"></i>
                </button>
                <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeFieldByName('${field.field_name}')" title="Delete">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </div>
    `;

    // Add drag event listeners
    fieldDiv.addEventListener('dragstart', handleDragStart);
    fieldDiv.addEventListener('dragend', handleDragEnd);

    return fieldDiv;
}

// Get field type badge
function getFieldTypeBadge(fieldType) {
    const badges = {
        'text': '<span class="badge bg-primary ms-2">text</span>',
        'keyword': '<span class="badge bg-secondary ms-2">keyword</span>',
        'long': '<span class="badge bg-success ms-2">long</span>',
        'integer': '<span class="badge bg-success ms-2">integer</span>',
        'double': '<span class="badge bg-success ms-2">double</span>',
        'float': '<span class="badge bg-success ms-2">float</span>',
        'date': '<span class="badge bg-warning ms-2">date</span>',
        'boolean': '<span class="badge bg-info ms-2">boolean</span>',
        'nested': '<span class="badge bg-dark ms-2">nested</span>',
        'object': '<span class="badge bg-secondary ms-2">object</span>',
        'join': '<span class="badge bg-purple ms-2">join</span>'
    };
    return badges[fieldType] || `<span class="badge bg-light text-dark ms-2">${fieldType}</span>`;
}

// Remove field by name
function removeFieldByName(fieldName) {
    if (confirm(`Are you sure you want to remove field "${fieldName}"?`)) {
        mappingFields = mappingFields.filter(field => field.field_name !== fieldName);
        updateMappingBuilderDisplay();
        updateElasticsearchMappingPreview();
        showAlert(`Field "${fieldName}" removed successfully`, 'success');
    }
}

// Drag and drop handlers
function handleDragStart(e) {
    draggedElement = e.target;
    e.target.classList.add('dragging');
    e.dataTransfer.setData('text/plain', e.target.dataset.fieldName);
}

function handleDragEnd(e) {
    e.target.classList.remove('dragging');
    draggedElement = null;
}

function handleDragOver(e) {
    e.preventDefault();
    e.currentTarget.classList.add('drag-over');
}

function handleDrop(e, targetSection) {
    e.preventDefault();
    e.currentTarget.classList.remove('drag-over');

    const fieldName = e.dataTransfer.getData('text/plain');
    const field = mappingFields.find(f => f.field_name === fieldName);

    if (field && field.section !== targetSection) {
        field.section = targetSection;

        // Update field type based on section
        if (targetSection === 'nested' && field.field_type !== 'nested') {
            field.field_type = 'nested';
            field.is_nested = true;
        } else if (targetSection === 'parent-child' && field.field_type !== 'join') {
            field.field_type = 'join';
            field.properties = { "type": "join", "relations": {} };
        }

        updateMappingBuilderDisplay();
        updateElasticsearchMappingPreview();
        showAlert(`Field "${fieldName}" moved to ${targetSection} section`, 'success');
    }
}

// Show saved mappings modal
function showSavedMappings() {
    const modal = new bootstrap.Modal(document.getElementById('savedMappingsModal'));
    loadSavedMappings();
    modal.show();
}

// Refresh saved mappings
function refreshSavedMappings() {
    loadSavedMappings();
}



// Display saved mappings with delete option
function displaySavedMappingsWithDelete(mappings) {
    const container = document.getElementById('savedMappingsList');
    if (!container) return;

    if (mappings.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted">
                <i class="fas fa-save fa-3x mb-3"></i>
                <p>No saved mappings found</p>
            </div>
        `;
        return;
    }

    container.innerHTML = mappings.map(mapping => `
        <div class="card mb-3">
            <div class="card-header d-flex justify-content-between align-items-center">
                <h6 class="mb-0">${mapping.mapping_name}</h6>
                <div class="btn-group" role="group">
                    <button type="button" class="btn btn-sm btn-outline-primary" onclick="viewSavedMapping(${mapping.id})" title="View">
                        <i class="fas fa-eye"></i>
                    </button>
                    <button type="button" class="btn btn-sm btn-outline-success" onclick="downloadSavedMapping(${mapping.id}, '${mapping.mapping_name}')" title="Download">
                        <i class="fas fa-download"></i>
                    </button>
                    <button type="button" class="btn btn-sm btn-outline-danger" onclick="confirmDeleteMapping(${mapping.id}, '${mapping.mapping_name}')" title="Delete">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
            <div class="card-body">
                <small class="text-muted">Environment: ${mapping.env_id} | Index: ${mapping.index_name}</small>
                <div class="mt-2">
                    <small class="text-muted">Preview:</small>
                    <pre class="small bg-light p-2 mt-1" style="max-height: 100px; overflow-y: auto;">${JSON.stringify(JSON.parse(mapping.mapping_json), null, 2).substring(0, 200)}${JSON.stringify(JSON.parse(mapping.mapping_json), null, 2).length > 200 ? '...' : ''}</pre>
                </div>
            </div>
        </div>
    `).join('');
}



function copyToClipboard(text, buttonId) {
    navigator.clipboard.writeText(text).then(() => {
        const button = document.getElementById(buttonId);
        if (button) {
            const originalText = button.innerHTML;
            button.innerHTML = '<i class="fas fa-check me-1"></i>Copied!';
            button.classList.remove('btn-outline-secondary');
            button.classList.add('btn-success');

            setTimeout(() => {
                button.innerHTML = originalText;
                button.classList.remove('btn-success');
                button.classList.add('btn-outline-secondary');
            }, 2000);
        }
    }).catch(err => {
        console.error('Failed to copy to clipboard:', err);
        showAlert('Failed to copy to clipboard', 'danger');
    });
}

function downloadMapping(mappingJson, filename) {
    const blob = new Blob([mappingJson], { type: 'application/json' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);

    URL.revokeObjectURL(url);
}



// ================================
// UI Field Mapping Form Builder
// ================================

let formBuilderData = {
    environment: null,
    index: null,
    fields: [],
    formConfig: {}
};


function initializeFormBuilder() {
    setupFormBuilderEventListeners();
    loadEnvironmentsForFormBuilder();
    loadSavedForms();
}

function setupFormBuilderEventListeners() {
    const formEnvironment = document.getElementById('formEnvironment');
    const formIndex = document.getElementById('formIndex');


    const saveFormConfig = document.getElementById('saveFormConfig');


    if (formIndex) {
        formIndex.addEventListener('change', handleFormIndexChange);
    }



    if (saveFormConfig) {
        saveFormConfig.addEventListener('click', handleSaveFormConfig);
    }
}

async function loadEnvironmentsForFormBuilder() {
    try {
        const response = await fetch('/environments');
        const environments = await response.json();

        const formEnvironment = document.getElementById('formEnvironment');
        formEnvironment.innerHTML = '<option value="">Select Environment...</option>';

        // Add Elasticsearch environments only (forms work with ES indices)
        const elasticsearchEnvs = environments.elasticsearch || [];
        elasticsearchEnvs.forEach(env => {
            const option = document.createElement('option');
            option.value = env.id;
            option.textContent = `${env.name} (${env.host_url})`;
            formEnvironment.appendChild(option);
        });

    } catch (error) {
        console.error('Error loading environments:', error);
        showAlert('Error loading environments: ' + error.message, 'danger');
    }
}

async function handleFormEnvironmentChange() {
    const envId = document.getElementById('formEnvironment').value;
    const formEnvironment = document.getElementById('formEnvironment');
    const formIndex = document.getElementById('formIndex');
    const loadIndexFields = document.getElementById('loadIndexFields');

    if (!envId) {
        formIndex.disabled = true;
        loadIndexFields.disabled = true;
        formIndex.innerHTML = '<option value="">Select Index...</option>';
        return;
    }

    try {
        showLoading('formEnvironment');
        const response = await fetch(`/indices/${envId}`);
        const result = await response.json();

        formIndex.innerHTML = '<option value="">Select Index...</option>';

        if (Array.isArray(result) && result.length > 0) {
            result.forEach(indexInfo => {
                const option = document.createElement('option');
                option.value = indexInfo.index;
                option.textContent = `${indexInfo.index} (${indexInfo["docs.count"]} docs)`;
                formIndex.appendChild(option);
            });
            formIndex.disabled = false;
        } else {
            showAlert('No indices found for selected environment', 'warning');
        }

    } catch (error) {
        console.error('Error loading indices:', error);
        showAlert('Error loading indices: ' + error.message, 'danger');
    } finally {
        hideLoading('formEnvironment');
    }
}

function handleFormIndexChange() {
    const formIndex = document.getElementById('formIndex');
    const loadIndexFields = document.getElementById('loadIndexFields');

    const indexName = formIndex.value;
    loadIndexFields.disabled = !indexName;

    if (indexName) {
        formBuilderData.index = indexName;
        formBuilderData.environment = document.getElementById('formEnvironment').value;
    }
}


async function handleLoadIndexFields() {
    const envId = enhancedFormBuilderData.environment;
    const indexName = enhancedFormBuilderData.index;

    if (!envId || !indexName) {
        showAlert('Please select both environment and index', 'warning');
        return;
    }

    try {
        showLoading('loadIndexFields');
        const response = await fetch(`/mapping/${envId}/${indexName}`);
        const result = await response.json();

        if (result.mapping) {
            const mappingKey = Object.keys(result.mapping)[0];
            const properties = result.mapping[mappingKey]?.mappings?.properties;

            if (properties) {
                const fields = extractFieldsFromMapping({ properties });
                enhancedFormBuilderData.availableFields = fields;
                displayEnhancedAvailableFields(fields);
                showFormConfigSection();
            }
        } else {
            showAlert('Failed to load index mapping: ' + (result.error || 'Unknown error'), 'danger');
        }

    } catch (error) {
        console.error('Error loading index fields:', error);
        showAlert('Error loading index fields: ' + error.message, 'danger');
    } finally {
        hideLoading('loadIndexFields');
    }
}

async function handleLoadIndexFields_v1() {
    const envId = document.getElementById('formEnvironment').value;
    const indexName = document.getElementById('formIndex').value;

    if (!envId || !indexName) {
        showAlert('Please select both environment and index', 'warning');
        return;
    }

    try {
        showLoading('loadIndexFields');
        const response = await fetch(`/mapping/${envId}/${indexName}`);
        const result = await response.json();

        if (result.mapping) {
            const mappingKey = Object.keys(result.mapping)[0];
            const properties = result.mapping?.[mappingKey]?.mappings;
            const fields = extractFieldsFromMapping(properties);
            formBuilderData.fields = fields;
            displayAvailableFields(fields);
            showFormConfigSection();
        } else {
            showAlert('Failed to load index mapping: ' + (result.error || 'Unknown error'), 'danger');
        }

    } catch (error) {
        console.error('Error loading index fields:', error);
        showAlert('Error loading index fields: ' + error.message, 'danger');
    } finally {
        hideLoading('loadIndexFields');
    }
}

function extractFieldsFromMapping(mapping) {
    const fields = [];

    function extractFields(properties, prefix = '') {
        for (const [fieldName, fieldConfig] of Object.entries(properties)) {
            const fullPath = prefix ? `${prefix}.${fieldName}` : fieldName;

            fields.push({
                name: fullPath,
                type: fieldConfig.type || 'object',
                originalConfig: fieldConfig
            });

            // Handle nested fields
            if (fieldConfig.properties) {
                extractFields(fieldConfig.properties, fullPath);
            }
        }
    }

    if (mapping.properties) {
        extractFields(mapping.properties);
    }

    return fields;
}

function displayAvailableFields(fields) {
    const availableFields = document.getElementById('availableFields');
    const fieldConfigPanel = document.getElementById('fieldConfigPanel');

    fieldConfigPanel.style.display = 'block';

    availableFields.innerHTML = '';

    fields.forEach(field => {
        const fieldElement = document.createElement('div');
        fieldElement.className = 'field-item mb-2 p-2 border rounded';
        fieldElement.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <strong>${field.name}</strong>
                    <span class="badge bg-secondary ms-2">${field.type}</span>
                </div>
                <button class="btn btn-sm btn-success" onclick="addFieldToForm('${field.name}', '${field.type}')">
                    <i class="fas fa-plus"></i>
                </button>
            </div>
        `;
        availableFields.appendChild(fieldElement);
    });
}

function addFieldToForm(fieldName, fieldType) {
    const formFields = document.getElementById('logicalFieldsBuilder');

    // Check if field already exists
    if (formBuilderData.formConfig[fieldName]) {
        showAlert('Field already added to form', 'warning');
        return;
    }

    // Create field configuration
    const fieldConfig = {
        name: fieldName,
        type: fieldType,
        inputType: 'text',
        role: 'value',
        required: false,
        placeholder: '',
        options: []
    };

    formBuilderData.formConfig[fieldName] = fieldConfig;

    // Create field configuration UI
    const fieldElement = document.createElement('div');
    fieldElement.className = 'field-config-item mb-3 p-3 border rounded';
    fieldElement.innerHTML = `
        <div class="d-flex justify-content-between align-items-center mb-2">
            <strong>${fieldName}</strong>
            <button class="btn btn-sm btn-danger" onclick="removeFieldFromForm('${fieldName}')">
                <i class="fas fa-trash"></i>
            </button>
        </div>
        <div class="row">
            <div class="col-md-6">
                <label class="form-label">Input Type</label>
                <select class="form-select" onchange="updateFieldConfig('${fieldName}', 'inputType', this.value)">
                    <option value="text">Text</option>
                    <option value="number">Number</option>
                    <option value="date">Date</option>
                    <option value="dropdown">Dropdown</option>
                    <option value="checkbox">Checkbox</option>
                    <option value="radio">Radio</option>
                </select>
            </div>
            <div class="col-md-6">
                <label class="form-label">Role</label>
                <select class="form-select" onchange="updateFieldConfig('${fieldName}', 'role', this.value)">
                    <option value="value">Value Field</option>
                    <option value="key">Key Field</option>
                </select>
            </div>
        </div>
        <div class="row mt-2">
            <div class="col-md-6">
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" onchange="updateFieldConfig('${fieldName}', 'required', this.checked)">
                    <label class="form-check-label">Required</label>
                </div>
            </div>
            <div class="col-md-6">
                <label class="form-label">Placeholder</label>
                <input type="text" class="form-control" onchange="updateFieldConfig('${fieldName}', 'placeholder', this.value)">
            </div>
        </div>
    `;

    formFields.appendChild(fieldElement);
    showFormConfigSection();
    updateFormPreview();
}

function removeFieldFromForm(fieldName) {
    delete formBuilderData.formConfig[fieldName];

    // Remove from UI
    const fieldElements = document.querySelectorAll('.field-config-item');
    fieldElements.forEach(element => {
        if (element.innerHTML.includes(fieldName)) {
            element.remove();
        }
    });

    updateFormPreview();
}

function updateFieldConfig(fieldName, property, value) {
    if (formBuilderData.formConfig[fieldName]) {
        formBuilderData.formConfig[fieldName][property] = value;
        updateFormPreview();
    }
}

function showFormConfigSection() {
    const formConfigSection = document.getElementById('formConfigSection');
    formConfigSection.style.display = 'block';
}

function updateFormPreview() {
    const formPreview = document.getElementById('formPreview');

    if (Object.keys(formBuilderData.formConfig).length === 0) {
        formPreview.innerHTML = '<p class="text-muted">Configure fields to see form preview</p>';
        return;
    }

    let previewHtml = '<form class="row">';

    Object.entries(formBuilderData.formConfig).forEach(([fieldName, config]) => {
        const required = config.required ? 'required' : '';
        const placeholder = config.placeholder || `Enter ${fieldName}`;

        previewHtml += `
            <div class="col-md-6 mb-3">
                <label class="form-label">${fieldName} ${config.required ? '<span class="text-danger">*</span>' : ''}</label>
        `;

        switch (config.inputType) {
            case 'text':
                previewHtml += `<input type="text" class="form-control" placeholder="${placeholder}" ${required}>`;
                break;
            case 'number':
                previewHtml += `<input type="number" class="form-control" placeholder="${placeholder}" ${required}>`;
                break;
            case 'date':
                previewHtml += `<input type="date" class="form-control" ${required}>`;
                break;
            case 'dropdown':
                previewHtml += `<select class="form-select" ${required}><option value="">Select ${fieldName}...</option></select>`;
                break;
            case 'checkbox':
                formHTML += `<div class="checkbox-enhanced-container">`;
                formHTML += `<label class="form-label">${field.label || field.name}</label>`;

                // Create text input with operator icon for multi-value selection
                formHTML += `<div class="input-group">`;
                formHTML += `<input type="text" class="form-control checkbox-values-display" 
                               id="checkbox-values-${field.name}" 
                               placeholder="Click icon to select values..." 
                               readonly>`;
                formHTML += `<button class="btn btn-outline-secondary" type="button" 
                               onclick="showMultiValueModal('${field.name}', '${field.sourceIndex || ''}', '${field.keyField || ''}', '${field.valueField || ''}')">`;
                formHTML += `<i class="fas fa-cog"></i>`;
                formHTML += `</button>`;
                formHTML += `</div>`;
                formHTML += `<input type="hidden" id="checkbox-selected-${field.name}" name="${field.name}">`;
                formHTML += `</div>`;
                break;

            case 'radio':
                previewHtml += `<div class="form-check"><input class="form-check-input" type="radio" name="${fieldName}" ${required}><label class="form-check-label">Option 1</label></div>`;
                break;
        }

        previewHtml += '</div>';
    });

    previewHtml += '<div class="col-12"><button type="submit" class="btn btn-primary">Search</button></div></form>';

    formPreview.innerHTML = previewHtml;
}

async function handleSaveFormConfig() {
    const formName = document.getElementById('formName').value;
    const formUrl = document.getElementById('formUrl').value;
    const envId = enhancedFormBuilderData.environment//document.getElementById('formEnvironment').value;
    const indexName = document.getElementById('formIndex').value;

    if (!formName || !formUrl || !envId || !indexName) {
        showAlert('Please fill in all form configuration fields', 'warning');
        return;
    }

    if (Object.keys(enhancedFormBuilderData.formConfig).length === 0) {
        showAlert('Please add at least one field to the form', 'warning');
        return;
    }

    try {
        showLoading('saveFormConfig');

        const response = await fetch('/save-form', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                name: formName,
                url: formUrl,
                environment: parseInt(envId),
                index: indexName,
                fields: enhancedFormBuilderData.formConfig
            })
        });

        const result = await response.json();

        if (result.success) {
            showAlert('Form configuration saved successfully!', 'success');
            loadSavedForms();

            // Clear form
            document.getElementById('formName').value = '';
            document.getElementById('formUrl').value = '';
            document.getElementById('logicalFieldsBuilder').innerHTML = '<p class="text-muted">Configure fields for your form</p>';
            formBuilderData.formConfig = {};
            updateFormPreview();
        } else {
            showAlert('Failed to save form: ' + (result.error || 'Unknown error'), 'danger');
        }

    } catch (error) {
        console.error('Error saving form:', error);
        showAlert('Error saving form: ' + error.message, 'danger');
    } finally {
        hideLoading('saveFormConfig');
    }
}

async function loadSavedForms() {
    try {
        const response = await fetch('/saved-forms');
        const result = await response.json();

        const savedFormsList = document.getElementById('savedFormsList');

        if (result.success && result.forms.length > 0) {
            savedFormsList.innerHTML = '';

            result.forms.forEach(form => {
                const formElement = document.createElement('div');
                formElement.className = 'saved-form-item mb-2 p-2 border rounded';
                formElement.innerHTML = `
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <strong>${form.name}</strong>
                            <br>
                            <small class="text-muted">${form.environment_name} → ${form.index_name}</small>
                        </div>
                        <div class="btn-group">
                            <a href="/form/${form.url}" target="_blank" class="btn btn-sm btn-primary">
                                <i class="fas fa-external-link-alt"></i>
                            </a>
                            <button class="btn btn-sm btn-danger" onclick="deleteForm(${form.id})">
                                <i class="fas fa-trash"></i>
                            </button>
                        </div>
                    </div>
                `;
                savedFormsList.appendChild(formElement);
            });
        } else {
            savedFormsList.innerHTML = '<p class="text-muted text-center">No saved forms yet</p>';
        }

    } catch (error) {
        console.error('Error loading saved forms:', error);
        showAlert('Error loading saved forms: ' + error.message, 'danger');
    }
}

async function deleteForm(formId) {
    if (!confirm('Are you sure you want to delete this form?')) {
        return;
    }

    try {
        const response = await fetch(`/delete-form/${formId}`, {
            method: 'DELETE'
        });

        const result = await response.json();

        if (result.success) {
            showAlert('Form deleted successfully!', 'success');
            loadSavedForms();
        } else {
            showAlert('Failed to delete form: ' + (result.error || 'Unknown error'), 'danger');
        }

    } catch (error) {
        console.error('Error deleting form:', error);
        showAlert('Error deleting form: ' + error.message, 'danger');
    }
}



async function showMultiValueModal(fieldName, sourceIndex, keyField, valueField) {
    currentMultiValueField = fieldName;
    currentMultiValueData = {
        sourceIndex: sourceIndex,
        keyField: keyField,
        valueField: valueField
    };

    // Load available values from the source index
    await loadMultiValueOptions(fieldName, sourceIndex, keyField, valueField);

    // Show the modal
    const multiValueModal = new bootstrap.Modal(document.getElementById("multiValueModal"));
    multiValueModal.show();
}

async function loadMultiValueOptions(fieldName, sourceIndex, keyField, valueField) {
    const valuesContainer = document.getElementById("multiValueOptions");
    const operatorSelect = document.getElementById("multiValueOperator");

    if (!sourceIndex || !keyField || !valueField) {
        valuesContainer.innerHTML = `
            <div class="alert alert-warning">
                <i class="fas fa-exclamation-triangle me-2"></i>
                Please configure source index, key field, and value field for this checkbox.
            </div>
        `;
        return;
    }

    try {
        // Get environment ID
        const envId = formBuilderData.environment;
        if (!envId) {
            throw new Error("No environment selected");
        }

        // Load index mapping to get available values
        const response = await fetch(`/mapping/${envId}/${sourceIndex}`);
        const mapping = await response.json();

        // Generate sample values based on field type
        const sampleValues = generateSampleValues(keyField, valueField);

        // Create checkboxes for each value
        valuesContainer.innerHTML = "";
        sampleValues.forEach((value, index) => {
            const checkboxItem = document.createElement("div");
            checkboxItem.className = "form-check mb-2";
            checkboxItem.innerHTML = `
                <input class="form-check-input multi-value-checkbox" 
                       type="checkbox" 
                       value="${value}" 
                       id="multi-value-${index}">
                <label class="form-check-label" for="multi-value-${index}">
                    ${value}
                </label>
            `;
            valuesContainer.appendChild(checkboxItem);
        });

        // Load existing selections if any
        loadExistingSelections(fieldName);

    } catch (error) {
        console.error("Error loading multi-value options:", error);
        valuesContainer.innerHTML = `
            <div class="alert alert-danger">
                <i class="fas fa-exclamation-circle me-2"></i>
                Error loading values: ${error.message}
            </div>
        `;
    }
}

function generateSampleValues(keyField, valueField) {
    // Generate realistic sample values based on field names
    const sampleData = {
        category: ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Automotive"],
        brand: ["Apple", "Samsung", "Nike", "Adidas", "Sony", "Microsoft"],
        status: ["Active", "Inactive", "Pending", "Approved", "Rejected"],
        type: ["Premium", "Standard", "Basic", "Professional", "Enterprise"],
        color: ["Red", "Blue", "Green", "Black", "White", "Yellow"],
        size: ["Small", "Medium", "Large", "Extra Large", "XXL"],
        price_range: ["$0-$50", "$50-$100", "$100-$200", "$200-$500", "$500+"],
        rating: ["1 Star", "2 Stars", "3 Stars", "4 Stars", "5 Stars"],
        location: ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
        default: ["Option 1", "Option 2", "Option 3", "Option 4", "Option 5"]
    };

    // Try to match field name to sample data
    const fieldKey = keyField.toLowerCase();
    for (const key in sampleData) {
        if (fieldKey.includes(key)) {
            return sampleData[key];
        }
    }

    return sampleData.default;
}

function loadExistingSelections(fieldName) {
    const hiddenInput = document.getElementById(`checkbox-selected-${fieldName}`);
    if (hiddenInput && hiddenInput.value) {
        try {
            const selections = JSON.parse(hiddenInput.value);

            // Set operator
            const operatorSelect = document.getElementById("multiValueOperator");
            if (operatorSelect && selections.operator) {
                operatorSelect.value = selections.operator;
            }

            // Set selected values
            if (selections.values && Array.isArray(selections.values)) {
                selections.values.forEach(value => {
                    const checkbox = document.querySelector(`input[value="${value}"]`);
                    if (checkbox) {
                        checkbox.checked = true;
                    }
                });
            }
        } catch (error) {
            console.error("Error loading existing selections:", error);
        }
    }
}

function applyMultiValueSelection() {
    if (!currentMultiValueField) return;

    const selectedValues = [];
    const checkboxes = document.querySelectorAll(".multi-value-checkbox:checked");
    const operator = document.getElementById("multiValueOperator").value;

    checkboxes.forEach(checkbox => {
        selectedValues.push(checkbox.value);
    });

    if (selectedValues.length === 0) {
        showAlert("Please select at least one value", "warning");
        return;
    }

    // Update the display text field
    const displayField = document.getElementById(`checkbox-values-${currentMultiValueField}`);
    const hiddenField = document.getElementById(`checkbox-selected-${currentMultiValueField}`);

    if (displayField && hiddenField) {
        // Create display text with operator
        const operatorText = operator === "AND" ? " AND " : operator === "OR" ? " OR " : " NOT ";
        const displayText = selectedValues.join(operatorText);

        // Update display field
        displayField.value = `${operator}: ${displayText}`;

        // Update hidden field with structured data
        const selectionData = {
            operator: operator,
            values: selectedValues,
            field: currentMultiValueField,
            sourceIndex: currentMultiValueData.sourceIndex,
            keyField: currentMultiValueData.keyField,
            valueField: currentMultiValueData.valueField
        };

        hiddenField.value = JSON.stringify(selectionData);

        // Close modal
        const multiValueModal = bootstrap.Modal.getInstance(document.getElementById("multiValueModal"));
        multiValueModal.hide();

        showAlert(`Selected ${selectedValues.length} values with ${operator} operator`, "success");
    }
}

function clearMultiValueSelection() {
    if (!currentMultiValueField) return;

    const displayField = document.getElementById(`checkbox-values-${currentMultiValueField}`);
    const hiddenField = document.getElementById(`checkbox-selected-${currentMultiValueField}`);

    if (displayField) displayField.value = "";
    if (hiddenField) hiddenField.value = "";

    // Clear all checkboxes
    const checkboxes = document.querySelectorAll(".multi-value-checkbox");
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });

    // Reset operator to default
    const operatorSelect = document.getElementById("multiValueOperator");
    if (operatorSelect) operatorSelect.value = "AND";

    showAlert("Selection cleared", "info");
}

function selectAllMultiValues() {
    const checkboxes = document.querySelectorAll(".multi-value-checkbox");
    checkboxes.forEach(checkbox => {
        checkbox.checked = true;
    });

    updateMultiValueCounter();
}

function deselectAllMultiValues() {
    const checkboxes = document.querySelectorAll(".multi-value-checkbox");
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });

    updateMultiValueCounter();
}

function updateMultiValueCounter() {
    const selectedCount = document.querySelectorAll(".multi-value-checkbox:checked").length;
    const totalCount = document.querySelectorAll(".multi-value-checkbox").length;

    const counterElement = document.getElementById("multiValueCounter");
    if (counterElement) {
        counterElement.textContent = `${selectedCount} of ${totalCount} selected`;
    }
}

// Add event listeners for multi-value checkboxes
document.addEventListener("change", function(e) {
    if (e.target.classList.contains("multi-value-checkbox")) {
        updateMultiValueCounter();
    }
});
// End of JavaScript file - All Oracle functionality and enhanced preview features implemented

// INSERT AT END OF FILE (AFTER LINE 3000+): Enhanced Form Builder Functions

/**
 * Initialize enhanced form builder
 */
function initializeEnhancedFormBuilder() {
    enhancedFormBuilderData = {
        environment: null,
        index: null,
        availableFields: [],
        formConfig: {},
        indexMappings: {}
    };

    loadEnvironmentsForFormBuilder();
    loadSavedForms();
}

/**
 * Display available fields with enhanced configuration options
 */
function displayEnhancedAvailableFields(fields) {
    const availableFields = document.getElementById('availableFields');
    const fieldConfigPanel = document.getElementById('fieldConfigPanel');

    fieldConfigPanel.style.display = 'block';
    availableFields.innerHTML = '';

    fields.forEach(field => {
        const fieldElement = document.createElement('div');
        fieldElement.className = 'field-item mb-2 p-2 border rounded bg-light';
        fieldElement.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <strong>${field.name}</strong>
                    <span class="badge bg-secondary ms-2">${field.type}</span>
                    <br>
                    <small class="text-muted">${field.path || 'Root level'}</small>
                </div>
                <button class="btn btn-sm btn-success" onclick="addEnhancedFieldToForm('${field.name}', '${field.type}')">
                    <i class="fas fa-plus"></i>
                </button>
            </div>
        `;
        availableFields.appendChild(fieldElement);
    });

    enhancedFormBuilderData.availableFields = fields;
}

/**
 * Add field to form with enhanced configuration
 */
function addEnhancedFieldToForm(fieldName, fieldType) {
    currentFieldName = fieldName;
    currentFieldConfig = {
        name: fieldName,
        type: fieldType,
        label: fieldName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
        inputType: 'text',
        operatorPreference: '==',
        role: 'value',
        required: false,
        placeholder: '',
        sourceIndex: '',
        keyField: '',
        valueField: '',
        operator: 'AND',
        selectedValues: []
    };

    showEnhancedFieldConfigModal();
}

/**
 * Show enhanced field configuration modal
 */
function showEnhancedFieldConfigModal() {
    if (!currentFieldConfig) return;

    // Populate form fields
    document.getElementById('fieldLabel').value = currentFieldConfig.label;
    document.getElementById('fieldInputType').value = currentFieldConfig.inputType;
    document.getElementById('fieldRole').value = currentFieldConfig.role;
    document.getElementById('fieldRequired').checked = currentFieldConfig.required;
    document.getElementById('fieldPlaceholder').value = currentFieldConfig.placeholder;

    // Load available indices for source configuration
    loadAvailableIndicesForSource();

    // Handle input type change
    handleInputTypeChange();

    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('enhancedFieldConfigModal'));
    modal.show();
}

/**
 * Handle input type change
 */
function handleInputTypeChange() {
    const inputType = document.getElementById('fieldInputType').value;
    const sourceConfigSection = document.getElementById('sourceConfigSection');
    const operatorSelect = document.getElementById('fieldOperator');

    if (inputType === 'dropdown' || inputType === 'checkbox' || inputType === 'radio') {
        sourceConfigSection.style.display = 'block';
    } else {
        sourceConfigSection.style.display = 'none';
    }

    // Populate operator options based on input type
    if (operatorSelect) {
        operatorSelect.innerHTML = '';
        const ops = operatorOptions[inputType] || { primary: [], secondary: [] };

        const addOptions = (group, label) => {
            if (group.length) {
                const optgroup = document.createElement('optgroup');
                optgroup.label = label;
                group.forEach(op => {
                    const option = document.createElement('option');
                    option.value = op;
                    option.textContent = op;
                    optgroup.appendChild(option);
                });
                operatorSelect.appendChild(optgroup);
            }
        };

        addOptions(ops.primary, 'Primary Operators');
        addOptions(ops.secondary, 'Secondary Operators');

        // Set selected operator
        const existing = currentFieldConfig?.operatorPreference;
        if (existing && operatorSelect.querySelector(`option[value="${existing}"]`)) {
            operatorSelect.value = existing;
        } else if (ops.primary.length) {
            operatorSelect.value = ops.primary[0];
        }
    }
}

/**
 * Load available indices for source configuration
 */
async function loadAvailableIndicesForSource() {
    const sourceIndexSelect = document.getElementById('sourceIndexSelect');
    const envId = enhancedFormBuilderData.environment;

    if (!envId) return;

    try {
        const response = await fetch(`/indices/${envId}`);
        const indices = await response.json();

        sourceIndexSelect.innerHTML = '<option value="">Select index...</option>';

        if (Array.isArray(indices)) {
            indices.forEach(index => {
                const option = document.createElement('option');
                option.value = index.index;
                option.textContent = `${index.index} (${index["docs.count"]} docs)`;
                sourceIndexSelect.appendChild(option);
            });
        }
    } catch (error) {
        console.error('Error loading indices:', error);
        showAlert('Error loading indices: ' + error.message, 'danger');
    }
}

/**
 * Load source index mapping and populate field selects
 */
async function loadSourceIndexMapping() {
    const sourceIndex = document.getElementById('sourceIndexSelect').value;
    const keyFieldSelect = document.getElementById('keyFieldSelect');
    const valueFieldSelect = document.getElementById('valueFieldSelect');
    const mappingFieldsList = document.getElementById('mappingFieldsList');

    if (!sourceIndex) {
        keyFieldSelect.innerHTML = '<option value="">Select key field...</option>';
        valueFieldSelect.innerHTML = '<option value="">Select value field...</option>';
        mappingFieldsList.innerHTML = '<p class="text-muted">Select an index to view mapping fields</p>';
        return;
    }

    try {
        const envId = enhancedFormBuilderData.environment;
        const response = await fetch(`/mapping/${envId}/${sourceIndex}`);
        const result = await response.json();

        if (result.mapping) {
            const mappingKey = Object.keys(result.mapping)[0];
            const properties = result.mapping[mappingKey]?.mappings?.properties;

            if (properties) {
                const fields = extractFieldsFromMapping({ properties });
                enhancedFormBuilderData.indexMappings[sourceIndex] = fields;

                // Populate field selects
                populateFieldSelects(fields, keyFieldSelect, valueFieldSelect);

                // Display mapping fields
                displayMappingFields(fields, mappingFieldsList);
            }
        }
    } catch (error) {
        console.error('Error loading source index mapping:', error);
        showAlert('Error loading mapping: ' + error.message, 'danger');
    }
}

/**
 * Populate key and value field selects
 */
function populateFieldSelects(fields, keyFieldSelect, valueFieldSelect) {
    keyFieldSelect.innerHTML = '<option value="">Select key field...</option>';
    valueFieldSelect.innerHTML = '<option value="">Select value field...</option>';

    fields.forEach(field => {
        // Key field option
        const keyOption = document.createElement('option');
        keyOption.value = field.name;
        keyOption.textContent = `${field.name} (${field.type})`;
        keyFieldSelect.appendChild(keyOption);

        // Value field option
        const valueOption = document.createElement('option');
        valueOption.value = field.name;
        valueOption.textContent = `${field.name} (${field.type})`;
        valueFieldSelect.appendChild(valueOption);
    });
}

/**
 * Display mapping fields in a selectable list
 */
function displayMappingFields(fields, container) {
    container.innerHTML = '';

    fields.forEach(field => {
        const fieldItem = document.createElement('div');
        fieldItem.className = 'mapping-field-item';
        fieldItem.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <span>${field.name}</span>
                <span class="field-type-badge badge bg-info">${field.type}</span>
            </div>
        `;

        fieldItem.addEventListener('click', () => {
            if (fieldItem.classList.contains('selected')) {
                fieldItem.classList.remove('selected');
            } else {
                container.querySelectorAll('.selected').forEach(el => el.classList.remove('selected'));
                fieldItem.classList.add('selected');
            }
        });

        container.appendChild(fieldItem);
    });
}

/**
 * Save enhanced field configuration
 */
function saveEnhancedFieldConfig() {
    const fieldLabel = document.getElementById('fieldLabel').value.trim();
    const inputType = document.getElementById('fieldInputType').value;
    const operator = document.getElementById('fieldOperator').value;
    const role = document.getElementById('fieldRole').value;
    const required = document.getElementById('fieldRequired').checked;
    const placeholder = document.getElementById('fieldPlaceholder').value.trim();

    if (!fieldLabel) {
        showAlert('Please enter a field label', 'warning');
        return;
    }

    // Update current field config
    currentFieldConfig.label = fieldLabel;
    currentFieldConfig.inputType = inputType;
    currentFieldConfig.operatorPreference = operator;
    currentFieldConfig.role = role;
    currentFieldConfig.required = required;
    currentFieldConfig.placeholder = placeholder;

    // Save source configuration if applicable
    if (inputType === 'dropdown' || inputType === 'checkbox' || inputType === 'radio') {
        currentFieldConfig.sourceIndex = document.getElementById('sourceIndexSelect').value;
        currentFieldConfig.keyField = document.getElementById('keyFieldSelect').value;
        currentFieldConfig.valueField = document.getElementById('valueFieldSelect').value;
    }

    // Add to form configuration
    enhancedFormBuilderData.formConfig[currentFieldName] = currentFieldConfig;

    // Update form fields display
    updateFormFieldsDisplay();

    // Update form preview
    updateEnhancedFormPreview();

    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('enhancedFieldConfigModal'));
    modal.hide();

    showAlert(`Field "${fieldLabel}" configured successfully`, 'success');
}

/**
 * Update form fields display
 */
function updateFormFieldsDisplay() {
    const formFields = document.getElementById('logicalFieldsBuilder');
    if (formFields) {
        formFields.innerHTML = '';
    } else {
        console.warn("#formFields not found in DOM");
    }


    if (Object.keys(enhancedFormBuilderData.formConfig).length === 0) {
        formFields.innerHTML = '<p class="text-muted">Configure fields for your form</p>';
        return;
    }

    Object.entries(enhancedFormBuilderData.formConfig).forEach(([fieldName, config]) => {
        const fieldElement = document.createElement('div');
        fieldElement.className = 'field-config-item mb-3 p-3 border rounded';
        fieldElement.innerHTML = `
            <div class="d-flex justify-content-between align-items-center mb-2">
                <div>
                    <strong>${config.label}</strong>
                    <span class="badge bg-primary ms-2">${config.inputType}</span>
                    ${config.role === 'key' ? '<span class="badge bg-warning ms-1">Key</span>' : ''}
                    ${config.required ? '<span class="badge bg-danger ms-1">Required</span>' : ''}
                </div>
                <div class="btn-group">
                    <button class="btn btn-sm btn-outline-primary" onclick="editFieldConfig('${fieldName}')">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeEnhancedFieldFromForm('${fieldName}')">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
            <small class="text-muted">
                Field: ${fieldName} | Type: ${config.type}
                ${config.sourceIndex ? ` | Source: ${config.sourceIndex}` : ''}
            </small>
        `;
        formFields.appendChild(fieldElement);
    });
}

/**
 * Update enhanced form preview with checkbox icons
 */
function updateEnhancedFormPreview() {
    const formPreview = document.getElementById('formPreview');

    if (Object.keys(enhancedFormBuilderData.formConfig).length === 0) {
        formPreview.innerHTML = '<p class="text-muted">Configure fields to see form preview</p>';
        return;
    }

    let formHTML = '<form class="row">';

    Object.entries(enhancedFormBuilderData.formConfig).forEach(([fieldName, field]) => {
        const required = field.required ? 'required' : '';
        const requiredMark = field.required ? '<span class="text-danger">*</span>' : '';
        const placeholder = field.placeholder || `Enter ${field.label}`;

        formHTML += `<div class="col-md-6 mb-3">`;
        formHTML += `<label class="form-label">${field.label} ${requiredMark}</label>`;

        switch (field.inputType) {
            case 'text':
                formHTML += `<input type="text" class="form-control" placeholder="${placeholder}" ${required}>`;
                break;

            case 'number':
                formHTML += `<input type="number" class="form-control" placeholder="${placeholder}" ${required}>`;
                break;

            case 'number-range':
                formHTML += `<div class="input-group">`;
                formHTML += `<input type="number" class="form-control" placeholder="Min" ${required}>`;
                formHTML += `<span class="input-group-text">to</span>`;
                formHTML += `<input type="number" class="form-control" placeholder="Max" ${required}>`;
                formHTML += `</div>`;
                break;

            case 'date':
                formHTML += `<input type="date" class="form-control" ${required}>`;
                break;

            case 'date-range':
                formHTML += `<div class="input-group">`;
                formHTML += `<input type="date" class="form-control" ${required}>`;
                formHTML += `<span class="input-group-text">to</span>`;
                formHTML += `<input type="date" class="form-control" ${required}>`;
                formHTML += `</div>`;
                break;


            case 'dropdown':
                formHTML += `<select class="form-select" id="dropdown-${fieldName}" ${required}>`;
                formHTML += `<option value="">Loading options...</option>`;
                formHTML += `</select>`;

                // Load values dynamically after form render
                setTimeout(() => {
                    loadDropdownValues(fieldName, field,enhancedFormBuilderData);
                }, 100);
                break;


            case 'checkbox':
                // Enhanced checkbox with icon and operator display
                formHTML += `<div class="checkbox-enhanced-container">`;
                formHTML += `<div class="input-group">`;
                formHTML += `<input type="text" class="form-control checkbox-values-display" 
                               id="checkbox-values-${fieldName}" 
                               placeholder="Click icon to select values..." 
                               readonly>`;
                formHTML += `<button class="btn btn-outline-secondary checkbox-operator-icon" type="button" 
                               onclick="showEnhancedMultiValueModal('${fieldName}')" 
                               title="Configure multi-value selection">`;
                formHTML += `<i class="fas fa-cogs"></i>`;

                // Show operator badge if values are selected
                if (field.selectedValues && field.selectedValues.length > 0) {
                    const operatorClass = field.operator === 'AND' ? 'bg-success' :
                        field.operator === 'OR' ? 'bg-warning' : 'bg-danger';
                    formHTML += `<span class="operator-badge badge ${operatorClass}">${field.operator}</span>`;
                }

                formHTML += `</button>`;
                formHTML += `</div>`;
                formHTML += `<input type="hidden" id="checkbox-selected-${fieldName}" name="${fieldName}">`;
                formHTML += `</div>`;
                break;

            case 'radio':
                if (field.selectedValues && field.selectedValues.length > 0) {
                    field.selectedValues.forEach((value, index) => {
                        formHTML += `<div class="form-check">`;
                        formHTML += `<input class="form-check-input" type="radio" name="${fieldName}" value="${value}" ${required}>`;
                        formHTML += `<label class="form-check-label">${value}</label>`;
                        formHTML += `</div>`;
                    });
                } else {
                    formHTML += `<p class="text-muted">Configure source to show options</p>`;
                }
                break;
        }

        formHTML += `</div>`;
    });

    formHTML += `<div class="col-12">`;
    formHTML += `<button type="submit" class="btn btn-primary">`;
    formHTML += `<i class="fas fa-search me-2"></i>Search`;
    formHTML += `</button>`;
    formHTML += `</div>`;
    formHTML += `</form>`;

    formPreview.innerHTML = formHTML;
}


async function loadDropdownValues(fieldName, fieldConfig,enhancedFormBuilderData) {
    const dropdown = document.getElementById(`dropdown-${fieldName}`);

    if (!fieldConfig.keyField || !fieldConfig.valueField) {
        // Use pre-configured values if no source is specified
        populateDropdownWithStaticValues(dropdown, fieldConfig);
        return;
    }

    try {
        // Show loading state
        dropdown.innerHTML = '<option value="">Loading options...</option>';
        dropdown.disabled = true;

        const envId = enhancedFormBuilderData.environment;
        const index = enhancedFormBuilderData.index;
        const response = await fetch(`/field-values/${envId}/${index}/${fieldConfig.keyField}`);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const result = await response.json();

        if (result.success) {
            populateDropdownOptions(dropdown, result.values, fieldConfig);
        } else {
            throw new Error(result.error);
        }
    } catch (error) {
        console.error('Error loading dropdown values:', error);
        dropdown.innerHTML = `<option value="">Error loading options</option>`;
        showAlert(`Failed to load options for ${fieldConfig.label}: ${error.message}`, 'danger');
    } finally {
        dropdown.disabled = false;
    }
}

function populateDropdownOptions(dropdown, values, fieldConfig) {
    // Clear existing options
    dropdown.innerHTML = '';

    // Add default option
    const defaultOption = document.createElement('option');
    defaultOption.value = '';
    defaultOption.textContent = `Select ${fieldConfig.label}...`;
    dropdown.appendChild(defaultOption);

    // Add options from API response
    values.forEach(valueObj => {
        const option = document.createElement('option');
        option.value = valueObj.value;

        // Show value with document count if available
        if (valueObj.count) {
            option.textContent = `${valueObj.value} (${valueObj.count} docs)`;
        } else {
            option.textContent = valueObj.value;
        }

        dropdown.appendChild(option);
    });

    // Pre-select value if configured
    if (fieldConfig.defaultValue) {
        dropdown.value = fieldConfig.defaultValue;
    }
}


async function loadValuesForMultiSelection(fieldConfig) {
    const valuesGrid = document.getElementById('valuesSelectionGrid');

    if ( !fieldConfig.keyField || !fieldConfig.valueField) {
        // Show configuration error
        return;
    }

    try {
        const envId = enhancedFormBuilderData.environment;
        const index = enhancedFormBuilderData.index;
        // REST API call to get field values
        const response = await fetch(`/field-values/${envId}/${index}/${fieldConfig.keyField}`);
        const result = await response.json();

        if (result.success) {
            // Populate the selection grid with API response
            populateValueSelectionGrid(result.values);
        } else {
            showError(result.error);
        }
    } catch (error) {
        console.error('Error loading values:', error);
        showError('Failed to load values from source index');
    }
}

function populateValueSelectionGrid(values) {
    const valuesGrid = document.getElementById('valuesSelectionGrid');
    valuesGrid.innerHTML = '';

    values.forEach((valueObj, index) => {
        const valueItem = document.createElement('div');
        valueItem.className = 'value-checkbox-item';
        valueItem.innerHTML = `
            <div class="form-check">
                <input class="form-check-input value-checkbox" 
                       type="checkbox" 
                       value="${valueObj.value}" 
                       id="value-${index}"
                       onchange="updateSelectionCounter()">
                <label class="form-check-label" for="value-${index}">
                    ${valueObj.value}
                    
                </label>
            </div>
        `;
        valuesGrid.appendChild(valueItem);
    });

    updateSelectionCounter();
}
/**
 * Show enhanced multi-value selection modal
 */
async function showEnhancedMultiValueModal(fieldName) {
    currentMultiValueField = fieldName;
    const fieldConfig = enhancedFormBuilderData.formConfig[fieldName];

    if (!fieldConfig) return;

    // Load values from source index
    await loadValuesForMultiSelection(fieldConfig);

    // Set current operator
    if (fieldConfig.operator) {
        document.querySelector(`input[name="logicalOperator"][value="${fieldConfig.operator}"]`).checked = true;
    }

    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('enhancedMultiValueModal'));
    modal.show();
}





/**
 * Load values for multi-selection
 */


/**
 * Generate sample values based on field configuration
 */
function generateSampleValuesFromConfig(fieldConfig) {
    const fieldName = fieldConfig.keyField || fieldConfig.valueField || 'default';

    const sampleData = {
        category: ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Automotive", "Health", "Beauty"],
        brand: ["Apple", "Samsung", "Nike", "Adidas", "Sony", "Microsoft", "Google", "Amazon"],
        status: ["Active", "Inactive", "Pending", "Approved", "Rejected", "Draft", "Published"],
        type: ["Premium", "Standard", "Basic", "Professional", "Enterprise", "Free", "Trial"],
        color: ["Red", "Blue", "Green", "Black", "White", "Yellow", "Purple", "Orange"],
        size: ["XS", "Small", "Medium", "Large", "XL", "XXL", "XXXL"],
        priority: ["Low", "Medium", "High", "Critical", "Urgent"],
        region: ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"],
        default: ["Option 1", "Option 2", "Option 3", "Option 4", "Option 5", "Option 6", "Option 7", "Option 8"]
    };

    const lowerFieldName = fieldName.toLowerCase();
    for (const key in sampleData) {
        if (lowerFieldName.includes(key)) {
            return sampleData[key];
        }
    }

    return sampleData.default;
}

/**
 * Selection control functions
 */
function selectAllValues() {
    document.querySelectorAll('.value-checkbox').forEach(checkbox => {
        checkbox.checked = true;
    });
    updateSelectionCounter();
}

function deselectAllValues() {
    document.querySelectorAll('.value-checkbox').forEach(checkbox => {
        checkbox.checked = false;
    });
    updateSelectionCounter();
}

function invertSelection() {
    document.querySelectorAll('.value-checkbox').forEach(checkbox => {
        checkbox.checked = !checkbox.checked;
    });
    updateSelectionCounter();
}

function updateSelectionCounter() {
    const selectedCount = document.querySelectorAll('.value-checkbox:checked').length;
    const totalCount = document.querySelectorAll('.value-checkbox').length;

    const counter = document.getElementById('selectionCounter');
    if (counter) {
        counter.textContent = `${selectedCount} of ${totalCount} selected`;
    }

    updateSelectionSummary();
}

function updateSelectionSummary() {
    const selectedValues = Array.from(document.querySelectorAll('.value-checkbox:checked'))
        .map(cb => cb.value);
    const operator = document.querySelector('input[name="logicalOperator"]:checked')?.value || 'AND';

    const summary = document.getElementById('selectionSummary');
    const summaryContent = document.getElementById('summaryContent');

    if (selectedValues.length > 0) {
        summary.style.display = 'block';

        const operatorSymbol = operator === 'AND' ? ' AND ' :
            operator === 'OR' ? ' OR ' : ' NOT ';

        summaryContent.innerHTML = `
            <strong>Selected Values:</strong> ${selectedValues.join(operatorSymbol)}
            <br>
            <strong>Operator:</strong> ${operator}
            <br>
            <strong>Count:</strong> ${selectedValues.length} value(s)
        `;
    } else {
        summary.style.display = 'none';
    }
}

function filterValues() {
    const searchTerm = document.getElementById('valueSearchFilter').value.toLowerCase();
    const valueItems = document.querySelectorAll('.value-checkbox-item');

    valueItems.forEach(item => {
        const label = item.querySelector('label').textContent.toLowerCase();
        if (label.includes(searchTerm)) {
            item.style.display = 'block';
        } else {
            item.style.display = 'none';
        }
    });
}

/**
 * Apply enhanced selection
 */
function applyEnhancedSelection() {
    if (!currentMultiValueField) return;

    const selectedValues = Array.from(document.querySelectorAll('.value-checkbox:checked'))
        .map(cb => cb.value);
    const operator = document.querySelector('input[name="logicalOperator"]:checked')?.value || 'AND';

    if (selectedValues.length === 0) {
        showAlert('Please select at least one value', 'warning');
        return;
    }

    // Update field configuration
    const fieldConfig = enhancedFormBuilderData.formConfig[currentMultiValueField];
    fieldConfig.selectedValues = selectedValues;
    fieldConfig.operator = operator;

    // Update form preview
    updateEnhancedFormPreview();

    // Update the preview display
    const displayField = document.getElementById(`checkbox-values-${currentMultiValueField}`);
    if (displayField) {
        const operatorSymbol = operator === 'AND' ? ' AND ' :
            operator === 'OR' ? ' OR ' : ' NOT ';
        displayField.value = `${operator}: ${selectedValues.join(operatorSymbol)}`;
    }

    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('enhancedMultiValueModal'));
    modal.hide();

    showAlert(`Applied ${selectedValues.length} values with ${operator} operator`, 'success');
}

// Additional utility functions
function editFieldConfig(fieldName) {
    currentFieldName = fieldName;
    currentFieldConfig = enhancedFormBuilderData.formConfig[fieldName];
    showEnhancedFieldConfigModal();
}

function removeEnhancedFieldFromForm(fieldName) {
    if (confirm(`Remove field "${enhancedFormBuilderData.formConfig[fieldName].label}"?`)) {
        delete enhancedFormBuilderData.formConfig[fieldName];
        updateFormFieldsDisplay();
        updateEnhancedFormPreview();
        showAlert('Field removed successfully', 'info');
    }
}

// Enhanced load index fields handler



/**
 * Enhanced Field Configuration with Logical Operators
 */
function addFieldWithLogicalOperator(fieldName, fieldType) {
    // Check if this is the first field
    if (logicalFieldsStructure.length === 0) {
        // First field - no operator needed
        const fieldConfig = createFieldConfig(fieldName, fieldType);
        logicalFieldsStructure.push({
            type: 'field',
            config: fieldConfig,
            id: generateFieldId()
        });
    } else {
        // Show operator selection modal
        showLogicalOperatorModal(fieldName, fieldType);
    }

    updateLogicalFieldsDisplay();
    updateQueryStructurePreview();
    updateFormPreviewWithLogic();
}

function showLogicalOperatorModal(fieldName, fieldType) {
    // Store pending field data
    window.pendingField = { fieldName, fieldType };

    // Update current fields preview
    updateCurrentFieldsPreview();

    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('logicalOperatorModal'));
    modal.show();
}

function updateCurrentFieldsPreview() {
    const container = document.getElementById('currentFieldsPreview');
    container.innerHTML = '';

    logicalFieldsStructure.forEach((item, index) => {
        const itemDiv = document.createElement('div');
        itemDiv.className = 'mb-2 p-2 rounded';

        if (item.type === 'field') {
            itemDiv.className += ' bg-light border';
            itemDiv.innerHTML = `
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <strong>${item.config.label}</strong>
                        <span class="badge bg-primary ms-2">${item.config.inputType}</span>
                    </div>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeLogicalItem(${index})">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            `;
        } else if (item.type === 'operator') {
            itemDiv.className += ' bg-secondary text-white text-center';
            itemDiv.innerHTML = `
                <strong>${item.operator}</strong>
                <button class="btn btn-sm btn-outline-light ms-2" onclick="removeLogicalItem(${index})">
                    <i class="fas fa-times"></i>
                </button>
            `;
        }

        container.appendChild(itemDiv);
    });
}

function applyLogicalOperators() {
    const selectedOperator = document.querySelector('.operator-btn.active')?.dataset.operator || 'AND';
    const enableGrouping = document.getElementById('enableGrouping').checked;

    if (!window.pendingField) return;

    // Add operator
    logicalFieldsStructure.push({
        type: 'operator',
        operator: selectedOperator,
        id: generateFieldId(),
        grouped: enableGrouping
    });

    // Add the new field
    const fieldConfig = createFieldConfig(window.pendingField.fieldName, window.pendingField.fieldType);
    logicalFieldsStructure.push({
        type: 'field',
        config: fieldConfig,
        id: generateFieldId()
    });

    // Clear pending field
    window.pendingField = null;

    // Update displays
    updateLogicalFieldsDisplay();
    updateQueryStructurePreview();
    updateFormPreviewWithLogic();

    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('logicalOperatorModal'));
    modal.hide();

    showAlert(`Field added with ${selectedOperator} operator`, 'success');
}

function updateLogicalFieldsDisplay() {
    const container = document.getElementById('logicalFieldsBuilder');
    container.innerHTML = '';

    if (logicalFieldsStructure.length === 0) {
        container.innerHTML = '<p class="text-muted">Configure fields to build your search form</p>';
        return;
    }

    logicalFieldsStructure.forEach((item, index) => {
        const itemDiv = document.createElement('div');
        itemDiv.className = 'mb-3';

        if (item.type === 'field') {
            itemDiv.innerHTML = createFieldDisplayHTML(item, index);
        } else if (item.type === 'operator') {
            itemDiv.innerHTML = createOperatorDisplayHTML(item, index);
        }

        container.appendChild(itemDiv);
    });

    // Enable/disable add operator button


}

function createFieldDisplayHTML(item, index) {
    return `
        <div class="field-logic-item p-3 border rounded bg-light">
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <h6 class="mb-1">
                        <i class="fas fa-wpforms me-2 text-primary"></i>
                        ${item.config.label}
                    </h6>
                    <div>
                        <span class="badge bg-primary">${item.config.inputType}</span>
                        <span class="badge bg-secondary ms-1">${item.config.role}</span>
                        ${item.config.required ? '<span class="badge bg-danger ms-1">Required</span>' : ''}
                    </div>
                    <small class="text-muted">Field: ${item.config.name}</small>
                </div>
                <div class="btn-group">
                    <button class="btn btn-sm btn-outline-primary" onclick="editLogicalField(${index})">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeLogicalItem(${index})">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
        </div>
    `;
}

function createOperatorDisplayHTML(item, index) {
    const operatorClass = item.operator === 'AND' ? 'success' :
        item.operator === 'OR' ? 'warning' : 'danger';

    return `
        <div class="operator-logic-item text-center my-2">
            <div class="d-inline-block position-relative">
                <button class="btn btn-${operatorClass} btn-lg px-4" onclick="editOperator(${index})">
                    <i class="fas fa-${item.operator === 'AND' ? 'plus' : item.operator === 'OR' ? 'circle-dot' : 'times'} me-2"></i>
                    <strong>${item.operator}</strong>
                </button>
                <button class="btn btn-sm btn-outline-danger position-absolute top-0 end-0 translate-middle rounded-circle" 
                        onclick="removeLogicalItem(${index})" style="width: 25px; height: 25px;">
                    <i class="fas fa-times" style="font-size: 10px;"></i>
                </button>
            </div>
        </div>
    `;
}

function updateQueryStructurePreview() {
    const container = document.getElementById('queryStructurePreview');

    if (logicalFieldsStructure.length === 0) {
        container.innerHTML = '<i class="fas fa-info-circle me-2"></i>No fields configured yet';
        container.className = 'alert alert-info';
        return;
    }

    let queryText = '';
    logicalFieldsStructure.forEach((item, index) => {
        if (item.type === 'field') {
            queryText += `<span class="badge bg-primary me-1">${item.config.label}</span>`;
        } else if (item.type === 'operator') {
            queryText += ` <span class="badge bg-secondary mx-2">${item.operator}</span> `;
        }
    });

    container.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
            <div>
                <strong>Search Logic:</strong> ${queryText}
            </div>
            <button class="btn btn-sm btn-outline-primary" onclick="showAdvancedQueryBuilder()">
                <i class="fas fa-cogs me-1"></i>Advanced
            </button>
        </div>
    `;
    container.className = 'alert alert-success';
}

function updateFormPreviewWithLogic() {
    const formPreview = document.getElementById('formPreview');
    const searchLogicFlow = document.getElementById('searchLogicFlow');

    if (logicalFieldsStructure.length === 0) {
        formPreview.innerHTML = '<p class="text-muted">Configure fields to see form preview</p>';
        searchLogicFlow.innerHTML = '<p class="text-muted text-center">Add fields and operators to visualize search logic</p>';
        return;
    }

    // Generate form HTML
    let formHTML = '<form class="row" onsubmit="handleLogicalFormSubmit(event)">';

    // Add all fields
    const fields = logicalFieldsStructure.filter(item => item.type === 'field');
    fields.forEach(item => {
        formHTML += generateFieldHTML(item.config);
    });

    // Add submit button with logic info
    formHTML += `
        <div class="col-12 mt-3">
            <button type="submit" class="btn btn-primary btn-lg">
                <i class="fas fa-search me-2"></i>Search with Logical Operators
            </button>
            <small class="text-muted d-block mt-2">
                Search will use: ${generateLogicSummary()}
            </small>
        </div>
    `;
    formHTML += '</form>';

    formPreview.innerHTML = formHTML;

    // Update search logic flow visualization
    updateSearchLogicFlowVisualization();
}

function updateSearchLogicFlowVisualization() {
    const container = document.getElementById('searchLogicFlow');
    container.innerHTML = '';

    const flowDiv = document.createElement('div');
    flowDiv.className = 'd-flex flex-wrap align-items-center justify-content-center';

    logicalFieldsStructure.forEach((item, index) => {
        if (item.type === 'field') {
            const fieldDiv = document.createElement('div');
            fieldDiv.className = 'm-2 p-2 bg-primary text-white rounded text-center';
            fieldDiv.style.minWidth = '100px';
            fieldDiv.innerHTML = `
                <i class="fas fa-wpforms d-block mb-1"></i>
                <strong>${item.config.label}</strong>
                <br><small>${item.config.inputType}</small>
            `;
            flowDiv.appendChild(fieldDiv);
        } else if (item.type === 'operator') {
            const operatorDiv = document.createElement('div');
            operatorDiv.className = `m-2 p-2 bg-${item.operator === 'AND' ? 'success' : item.operator === 'OR' ? 'warning' : 'danger'} text-white rounded text-center`;
            operatorDiv.innerHTML = `
                <i class="fas fa-${item.operator === 'AND' ? 'plus' : item.operator === 'OR' ? 'circle-dot' : 'times'} d-block mb-1"></i>
                <strong>${item.operator}</strong>
            `;
            flowDiv.appendChild(operatorDiv);
        }
    });

    container.appendChild(flowDiv);
}

// Utility functions
function generateFieldId() {
    return 'field_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

function createFieldConfig(fieldName, fieldType) {
    return {
        name: fieldName,
        type: fieldType,
        label: fieldName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
        inputType: 'text',
        operatorPreference: '==',
        role: 'value',
        required: false,
        placeholder: `Enter ${fieldName}`
    };
}

function generateLogicSummary() {
    let summary = '';
    logicalFieldsStructure.forEach((item, index) => {
        if (item.type === 'field') {
            summary += item.config.label;
        } else if (item.type === 'operator') {
            summary += ` ${item.operator} `;
        }
    });
    return summary;
}

function removeLogicalItem(index) {
    logicalFieldsStructure.splice(index, 1);
    updateLogicalFieldsDisplay();
    updateQueryStructurePreview();
    updateFormPreviewWithLogic();
}

// Event listeners for operator buttons
document.addEventListener('DOMContentLoaded', function() {
    // Operator button selection
    document.addEventListener('click', function(e) {
        if (e.target.classList.contains('operator-btn') || e.target.closest('.operator-btn')) {
            const btn = e.target.classList.contains('operator-btn') ? e.target : e.target.closest('.operator-btn');

            // Remove active class from all operator buttons
            document.querySelectorAll('.operator-btn').forEach(b => b.classList.remove('active'));

            // Add active class to clicked button
            btn.classList.add('active');

            // Update query preview
            updateQueryLogicPreview();
        }
    });
});

function updateQueryLogicPreview() {
    const activeOperator = document.querySelector('.operator-btn.active')?.dataset.operator;
    const enableGrouping = document.getElementById('enableGrouping')?.checked;

    if (!activeOperator) return;

    let previewText = '';
    const fieldNames = logicalFieldsStructure
        .filter(item => item.type === 'field')
        .map(item => item.config.label);

    if (window.pendingField) {
        fieldNames.push(window.pendingField.fieldName);
    }

    if (fieldNames.length > 1) {
        if (enableGrouping) {
            previewText = `(${fieldNames.slice(0, -1).join(' AND ')}) ${activeOperator} (${fieldNames[fieldNames.length - 1]})`;
        } else {
            previewText = fieldNames.join(` ${activeOperator} `);
        }
    } else {
        previewText = fieldNames.join('');
    }

    const previewContainer = document.getElementById('queryLogicText');
    if (previewContainer) {
        previewContainer.textContent = previewText || 'Select fields and operators to see query preview';
    }
}

function handleLogicalFormSubmit(event) {
    event.preventDefault();

    const formData = new FormData(event.target);
    const fieldValues = {};

    // Extract field values
    for (const [key, value] of formData.entries()) {
        fieldValues[key] = value;
    }

    // Prepare submission data with logical structure
    const submissionData = {
        fieldValues: fieldValues,
        logicalStructure: logicalFieldsStructure
    };

    // Submit to enhanced endpoint
    submitFormWithLogic(submissionData);
}

async function submitFormWithLogic(submissionData) {
    try {
        const formUrl = window.location.pathname.split('/').pop();

        const response = await fetch(`/submit-form-with-logic/${formUrl}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(submissionData)
        });

        const result = await response.json();

        if (result.success) {
            displayLogicalSearchResults(result);
        } else {
            showAlert('Search failed: ' + result.error, 'danger');
        }
    } catch (error) {
        console.error('Form submission error:', error);
        showAlert('Form submission error: ' + error.message, 'danger');
    }
}

function displayLogicalSearchResults(result) {
    // Enhanced result display with logical structure info
    const resultsHtml = `
        <div class="search-results mt-4">
            <div class="d-flex justify-content-between align-items-center mb-3">
                <h5>Search Results</h5>
                <span class="badge bg-primary">${result.total} results found</span>
            </div>
            
            <div class="alert alert-info">
                <strong>Search Logic Used:</strong> ${generateLogicSummary()}
            </div>
            
            <div class="results-grid">
                ${result.results.map(item => `
                    <div class="result-item card mb-2">
                        <div class="card-body">
                            <h6>Score: ${item.score}</h6>
                            <pre class="small">${JSON.stringify(item.data, null, 2)}</pre>
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;

    document.getElementById('formPreview').innerHTML += resultsHtml;
}

function handleAddLogicalOperator() {
    const operator = document.getElementById('logicalOperatorSelect').value;
    if (operator) {
        operator.addEventListener('change', function() {
            updateLogicalOperatorButton();
            console.log('Operator selected:', this.value);
        });
    }
    if (!operator) {
        showAlert('Please select an operator first', 'warning');
        return;
    }
    showAlert(`${operator} operator added!`, 'success');
}

function updateLogicalOperatorButton() {
    const addOperatorBtn = document.getElementById('addLogicalOperator');
    if (!addOperatorBtn) return;

    const fieldCount = countFormConfigFields();
    const hasOperatorSelected = document.getElementById('logicalOperatorSelect')?.value !== '';
    const shouldEnable = fieldCount >= 2 && hasOperatorSelected;

    addOperatorBtn.disabled = !shouldEnable;

    // Update button text with field count
    const icon = '<i class="fas fa-plus me-2"></i>';
    if (fieldCount < 2) {
        addOperatorBtn.innerHTML = `${icon}Add Operator (${fieldCount}/2 fields)`;
        addOperatorBtn.className = 'btn btn-secondary w-100'; // Gray when disabled
    } else if (!hasOperatorSelected) {
        addOperatorBtn.innerHTML = `${icon}Add Operator (Select operator first)`;
        addOperatorBtn.className = 'btn btn-warning w-100'; // Yellow when operator needed
    } else {
        addOperatorBtn.innerHTML = `${icon}Add Operator Between Fields`;
        addOperatorBtn.className = 'btn btn-success w-100'; // Green when ready
    }
}

async function validateMappingModal() {
    const mappingJsonRaw = document.getElementById('mappingJson').textContent;

    if (!mappingJsonRaw || mappingJsonRaw.trim() === 'Add fields to see live mapping preview...') {
        showAlert('Please generate a mapping first before saving to Elasticsearch', 'warning');
        return;
    }

    let mappingObj;
    try {
        mappingObj = JSON.parse(mappingJsonRaw);
    } catch (e) {
        showAlert('Invalid JSON format in mapping preview.', 'danger');
        return;
    }

    // Now show modal AFTER validation passed
    const modalElement = new bootstrap.Modal(document.getElementById('validationModal'));
    document.getElementById('validationModalBody').innerHTML = `
        <div class="text-center py-5">
            <div class="spinner-border text-info" role="status">
                <span class="visually-hidden">Validating...</span>
            </div>
            <p class="mt-3">Validating mapping, please wait...</p>
        </div>
    `;
    modalElement.show();


    try {
        mappingObj = JSON.parse(mappingJsonRaw);
    } catch (e) {
        document.getElementById('validationModalBody').innerHTML = `
            <div class="alert alert-danger">
                <strong>Error:</strong> Invalid JSON format in mapping preview.
            </div>
        `;
        return;
    }

    const payload = {
        mapping: mappingObj,
        context: "e-commerce products"
    };

    try {
        const response = await fetch("/api/validate-mapping", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        const result = await response.json();

        const issueItems = result.issues.map(issue => `
            <li>
                <strong>Type:</strong> ${issue.type}<br>
                ${issue.field ? `<strong>Field:</strong> ${issue.field}<br>` : ""}
                <strong>Message:</strong> ${issue.message}<br>
                ${issue.recommendation ? `<strong>Recommendation:</strong> ${issue.recommendation}` : ""}
            </li>
        `).join("");

        document.getElementById('validationModalBody').innerHTML = `
            <p><strong>Is Valid:</strong> ${result.is_valid ? '✅ Yes' : '❌ No'}</p>
            <p><strong>Score:</strong> ${result.overall_score}</p>
            <p><strong>Summary:</strong> ${result.summary}</p>
            <h6>Issues:</h6>
            <ul>${issueItems}</ul>
        `;

    } catch (error) {
        document.getElementById('validationModalBody').innerHTML = `
            <div class="alert alert-danger">
                <strong>Error:</strong> ${error.message}
            </div>
        `;
    }
}



async function validateMappingModal() {
    const mappingJsonRaw = document.getElementById('mappingJson').textContent;

    if (!mappingJsonRaw || mappingJsonRaw.trim() === 'Add fields to see live mapping preview...') {
        showAlert('Please generate a mapping first before saving to Elasticsearch', 'warning');
        return;
    }

    let mappingObj;
    try {
        mappingObj = JSON.parse(mappingJsonRaw);
    } catch (e) {
        showAlert('Invalid JSON format in mapping preview.', 'danger');
        return;
    }

    // Now show modal AFTER validation passed
    const modalElement = new bootstrap.Modal(document.getElementById('validationModal'));
    document.getElementById('validationModalBody').innerHTML = `
        <div class="text-center py-5">
            <div class="spinner-border text-info" role="status">
                <span class="visually-hidden">Validating...</span>
            </div>
            <p class="mt-3">Validating mapping, please wait...</p>
        </div>
    `;
    modalElement.show();


    try {
        mappingObj = JSON.parse(mappingJsonRaw);
    } catch (e) {
        document.getElementById('validationModalBody').innerHTML = `
            <div class="alert alert-danger">
                <strong>Error:</strong> Invalid JSON format in mapping preview.
            </div>
        `;
        return;
    }

    const payload = {
        mapping: mappingObj,
        context: "e-commerce products"
    };

    try {
        const response = await fetch("/api/validate-mapping", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        const result = await response.json();

        const issueItems = result.issues.map(issue => `
            <li>
                <strong>Type:</strong> ${issue.type}<br>
                ${issue.field ? `<strong>Field:</strong> ${issue.field}<br>` : ""}
                <strong>Message:</strong> ${issue.message}<br>
                ${issue.recommendation ? `<strong>Recommendation:</strong> ${issue.recommendation}` : ""}
            </li>
        `).join("");

        document.getElementById('validationModalBody').innerHTML = `
            <p><strong>Is Valid:</strong> ${result.is_valid ? '✅ Yes' : '❌ No'}</p>
            <p><strong>Score:</strong> ${result.overall_score}</p>
            <p><strong>Summary:</strong> ${result.summary}</p>
            <h6>Issues:</h6>
            <ul>${issueItems}</ul>
        `;

    } catch (error) {
        document.getElementById('validationModalBody').innerHTML = `
            <div class="alert alert-danger">
                <strong>Error:</strong> ${error.message}
            </div>
        `;
    }
}


function initializeWorkflow() {
    if (!workflowInitialized) {
        setupWorkflowEventListeners();
        workflowInitialized = true;
    }
    loadOracleEnvironmentsForWorkflow();
    loadElasticEnvironmentsForWorkflow();
    updateStepVisibility();
}

async function loadElasticEnvironmentsForWorkflow() {
    try {
        const response = await fetch('/environments');
        const envs = await response.json();

        const select = document.getElementById('workflowElasticEnvironment');
        select.innerHTML = '<option value="">Select environment...</option>';

        if (envs.elasticsearch && envs.elasticsearch.length > 0) {
            envs.elasticsearch.forEach(env => {
                const option = document.createElement('option');
                option.value = env.id;
                option.textContent = `${env.name} (${env.host_url})`;
                select.appendChild(option);
            });
        }
    } catch (error) {
        console.error('Error loading Elasticsearch environments:', error);
        showAlert('Error loading Elasticsearch environments: ' + error.message, 'danger');
    }
}


function setupWorkflowEventListeners() {
    const envSelect = document.getElementById('workflowOracleEnvironment');
    const connectBtn = document.getElementById('connectWorkflowOracle');
    const loadStructuresBtn = document.getElementById('loadSelectedStructures');
    const autoDetectionBtn = document.getElementById('runAutoDetection');
    const generateBtn = document.getElementById('generateWorkflowMappingBtn');
    const saveBtn = document.getElementById('saveWorkflowMappingBtn');
    const previewBtn = document.getElementById('previewWorkflowMappingBtn');
    const fieldDataMappingBtn = document.getElementById('fieldDataMappingBtn');
    const saveFieldDataMappingBtn = document.getElementById('saveFieldDataMappingBtn');
    const dataLoadBtn = document.getElementById('dataLoadBtn');
    const runDataLoadBtn = document.getElementById('runDataLoadBtn');
    const dataLoadElasticEnv = document.getElementById('dataLoadElasticEnv');
    const mappingNameInput = document.getElementById('workflowMappingName');
    const indexNameInput = document.getElementById('workflowIndexName');
    const elasticEnvSelect = document.getElementById('workflowElasticEnvironment');

    if (envSelect) {
        envSelect.addEventListener('change', function() {
            connectBtn.disabled = !this.value;
            workflowData.selectedEnvironment = this.value;
        });
    }

    if (connectBtn) {
        connectBtn.addEventListener('click', connectToWorkflowOracle);
    }

    if (loadStructuresBtn) {
        loadStructuresBtn.addEventListener('click', loadSelectedTableStructures);
    }

    if (autoDetectionBtn) {
        autoDetectionBtn.addEventListener('click', runAutoDetection);
    }

    if (generateBtn) {
        generateBtn.addEventListener('click', () => generateWorkflowMapping(false));
    }
    if (saveBtn) {
        saveBtn.addEventListener('click', () => generateWorkflowMapping(true));
    }
    if (previewBtn) {
        previewBtn.addEventListener('click', showWorkflowMappingPreview);
    }
    if (fieldDataMappingBtn) {
        fieldDataMappingBtn.addEventListener('click', showFieldDataMappingModal);
    }
    if (saveFieldDataMappingBtn) {
        saveFieldDataMappingBtn.addEventListener('click', saveFieldDataMapping);
    }
    if (dataLoadBtn) {
        dataLoadBtn.addEventListener('click', showDataLoadModal);
    }
    if (runDataLoadBtn) {
        runDataLoadBtn.addEventListener('click', runDataLoad);
    }
    if (dataLoadElasticEnv) {
        dataLoadElasticEnv.addEventListener('change', handleDataLoadElasticEnvChange);
    }

    if (mappingNameInput) {
        mappingNameInput.addEventListener('input', () => {
            workflowGeneratedMapping = null;
            updateGenerateWorkflowButton();
        });
    }
    if (indexNameInput) {
        indexNameInput.addEventListener('input', () => {
            workflowGeneratedMapping = null;
            updateGenerateWorkflowButton();
        });
    }
    if (elasticEnvSelect) {
        elasticEnvSelect.addEventListener('change', () => {
            workflowGeneratedMapping = null;
            updateGenerateWorkflowButton();
        });
    }
}

async function loadOracleEnvironmentsForWorkflow() {
    try {
        const response = await fetch('/environments');
        const environments = await response.json();

        const select = document.getElementById('workflowOracleEnvironment');
        select.innerHTML = '<option value="">Choose Oracle Environment...</option>';

        if (environments.oracle && environments.oracle.length > 0) {
            environments.oracle.forEach(env => {
                const option = document.createElement('option');
                option.value = env.id;
                option.textContent = `${env.name} (${env.url})`;
                select.appendChild(option);
            });
        }
    } catch (error) {
        console.error('Error loading Oracle environments:', error);
        showAlert('Error loading Oracle environments: ' + error.message, 'danger');
    }
}

async function connectToWorkflowOracle() {
    if (!workflowData.selectedEnvironment) return;

    try {
        showLoading('connectWorkflowOracle');
        updateConnectionStatus('connecting');

        // Test connection
        const testResponse = await fetch(`/test-connection/oracle/${workflowData.selectedEnvironment}`, {
            method: 'POST'
        });
        const testResult = await testResponse.json();

        if (!testResult.success) {
            throw new Error(testResult.message);
        }

        // Load tables
        const tablesResponse = await fetch(`/oracle/workflow-tables/${workflowData.selectedEnvironment}`);
        const tablesData = await tablesResponse.json();

        if (tablesData.success) {
            updateConnectionStatus('connected', testResult);
            populateWorkflowTables(tablesData.tables);
            goToStep(2);
        } else {
            throw new Error(tablesData.error);
        }

    } catch (error) {
        updateConnectionStatus('error', null, error.message);
        showAlert('Connection failed: ' + error.message, 'danger');
    } finally {
        hideLoading('connectWorkflowOracle');
    }
}


function updateConnectionStatus(status, connectionInfo = null, errorMessage = null) {
    const container = document.getElementById('workflowConnectionStatus');

    switch (status) {
        case 'connecting':
            container.innerHTML = `
                <div class="text-center text-primary py-4">
                    <div class="spinner-border text-primary mb-3" role="status"></div>
                    <p>Connecting to Oracle environment...</p>
                </div>
            `;
            break;

        case 'connected':
            container.innerHTML = `
                <div class="text-center text-success py-4">
                    <i class="fas fa-check-circle fa-3x mb-3"></i>
                    <h6>Successfully Connected!</h6>
                    <p class="small text-muted">
                        ${connectionInfo ? `Database: ${connectionInfo.database || 'Oracle'}` : ''}
                    </p>
                </div>
            `;
            break;

        case 'error':
            container.innerHTML = `
                <div class="text-center text-danger py-4">
                    <i class="fas fa-exclamation-triangle fa-3x mb-3"></i>
                    <h6>Connection Failed</h6>
                    <p class="small">${errorMessage}</p>
                </div>
            `;
            break;
    }
}


function populateWorkflowTables(tables) {
    const container = document.getElementById('workflowTablesList');
    container.innerHTML = '';

    if (!tables || tables.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-5">
                <i class="fas fa-table fa-3x mb-3"></i>
                <p>No tables found in this Oracle environment</p>
            </div>
        `;
        return;
    }

    tables.forEach((table, index) => {
        const tableName = typeof table === 'string' ? table : table.table_name;
        const tableItem = document.createElement('div');
        tableItem.className = 'table-item';
        tableItem.innerHTML = `
            <div class="d-flex align-items-center">
                <input type="checkbox" class="form-check-input table-checkbox" 
                       value="${tableName}" id="table_${index}">
                <label for="table_${index}" class="form-check-label ms-2 flex-grow-1">
                    <strong>${tableName}</strong>
                </label>
                <div class="table-stats">
                    <i class="fas fa-table me-1"></i>Oracle Table
                </div>
            </div>
        `;

        // Add click event for selection
        tableItem.addEventListener('click', function(e) {
            if (e.target.type !== 'checkbox') {
                const checkbox = tableItem.querySelector('.table-checkbox');
                checkbox.checked = !checkbox.checked;
                handleTableSelection(checkbox);
            }
        });

        tableItem.querySelector('.table-checkbox').addEventListener('change', function() {
            handleTableSelection(this);
        });

        container.appendChild(tableItem);
    });
}

function handleTableSelection(checkbox) {
    const tableName = checkbox.value;
    const tableItem = checkbox.closest('.table-item');

    if (checkbox.checked) {
        if (!workflowData.selectedTables.includes(tableName)) {
            workflowData.selectedTables.push(tableName);
        }
        tableItem.classList.add('selected');
    } else {
        workflowData.selectedTables = workflowData.selectedTables.filter(t => t !== tableName);
        tableItem.classList.remove('selected');
    }

    updateTableSelectionSummary();
}


function updateTableSelectionSummary() {
    const count = workflowData.selectedTables.length;
    const countBadge = document.getElementById('selectedTablesCount');
    const summaryContainer = document.getElementById('tableSelectionSummary');
    const loadBtn = document.getElementById('loadSelectedStructures');

    countBadge.textContent = `${count} selected`;

    if (count === 0) {
        summaryContainer.innerHTML = '<p class="text-muted">No tables selected yet</p>';
        loadBtn.disabled = true;
    } else {
        let summaryHTML = `<h6 class="text-primary">${count} Table(s) Selected:</h6><ul class="list-unstyled">`;
        workflowData.selectedTables.forEach(table => {
            summaryHTML += `<li><i class="fas fa-table me-2 text-danger"></i>${table}</li>`;
        });
        summaryHTML += '</ul>';
        summaryContainer.innerHTML = summaryHTML;
        loadBtn.disabled = false;
    }
}


function selectAllTables() {
    const checkboxes = document.querySelectorAll('.table-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = true;
        handleTableSelection(checkbox);
    });
}

function clearTableSelection() {
    const checkboxes = document.querySelectorAll('.table-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
        handleTableSelection(checkbox);
    });
}

async function loadSelectedTableStructures() {
    if (workflowData.selectedTables.length === 0) return;

    try {
        showLoading('loadSelectedStructures');
        const container = document.getElementById('tableStructuresContainer');
        container.innerHTML = `
            <div class="text-center py-5">
                <div class="spinner-border text-primary mb-3"></div>
                <p>Loading table structures...</p>
            </div>
        `;

        // Load structures for all selected tables
        const promises = workflowData.selectedTables.map(tableName =>
            fetch(`/oracle/table-structure/${workflowData.selectedEnvironment}/${tableName}`)
                .then(response => response.json())
                .then(result => ({ tableName, result }))
        );

        const results = await Promise.all(promises);

        // Process results
        container.innerHTML = '';
        results.forEach(({ tableName, result }) => {
            if (result.success) {
                workflowData.tableStructures[tableName] = result.columns;
                displayTableStructure(tableName, result.columns, container);
            }
        });

        goToStep(3);
        completeStep(3);

    } catch (error) {
        showAlert('Error loading table structures: ' + error.message, 'danger');
    } finally {
        hideLoading('loadSelectedStructures');
    }
}
function displayTableStructure(tableName, columns, container) {
    const structureCard = document.createElement('div');
    structureCard.className = 'card mb-3 fade-in';
    structureCard.innerHTML = `
        <div class="card-header bg-danger text-white">
            <h6 class="mb-0">
                <i class="fas fa-table me-2"></i>${tableName}
                <span class="badge bg-light text-dark ms-2">${columns.length} columns</span>
            </h6>
        </div>
        <div class="card-body">
            <div class="row">
                ${columns.slice(0, 6).map(col => `
                    <div class="col-md-4 mb-2">
                        <div class="oracle-field">
                            <strong>${col.name || col.column_name}</strong>
                            <br><small class="text-muted">${col.type || col.data_type}</small>
                        </div>
                    </div>
                `).join('')}
                ${columns.length > 6 ? `
                    <div class="col-12">
                        <small class="text-muted">... and ${columns.length - 6} more columns</small>
                    </div>
                ` : ''}
            </div>
        </div>
    `;
    container.appendChild(structureCard);
}

async function runAutoDetection() {
    const query = document.getElementById('autoDetectionQuery').value.trim();

    if (!query) {
        showAlert('Please enter a JOIN query for auto-detection', 'warning');
        return;
    }

    try {
        showLoading('runAutoDetection');

        const formData = new FormData();
        formData.append('query', query);
        formData.append('tables', JSON.stringify(workflowData.selectedTables));

        const response = await fetch(`/oracle/auto-detect-relationships/${workflowData.selectedEnvironment}`, {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (result.success) {
            workflowData.detectionResults = result.relationships.map(r => ({
                ...r,
                type: r.type || 'nested'
            }));
            displayDetectionResults(workflowData.detectionResults);

            const modal = new bootstrap.Modal(document.getElementById('autoDetectionModal'));
            modal.show();
        } else {
            throw new Error(result.error);
        }

    } catch (error) {
        showAlert('Auto-detection failed: ' + error.message, 'danger');
    } finally {
        hideLoading('runAutoDetection');
    }
}

function displayDetectionResults(relationships) {
    const container = document.getElementById('detectionResults');

    if (!relationships || relationships.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-5">
                <i class="fas fa-search fa-3x mb-3"></i>
                <p>No relationships detected in the query</p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <div class="mb-3">
            <h6><i class="fas fa-magic me-2"></i>Detected Relationships (${relationships.length})</h6>
        </div>
    `;

    relationships.forEach((rel, index) => {
        const confidence = rel.confidence || Math.random() * 0.4 + 0.6; // Mock confidence
        const confidenceClass = confidence > 0.8 ? 'confidence-high' :
            confidence > 0.5 ? 'confidence-medium' : 'confidence-low';

        const resultDiv = document.createElement('div');
        resultDiv.className = 'detection-result';
        resultDiv.innerHTML = `
            <div class="d-flex justify-content-between align-items-start">
                <div>
                    <h6>
                        <i class="fas fa-link me-2 text-primary"></i>
                        ${rel.parentTable}.${rel.parentField} → ${rel.childTable}.${rel.childField}
                    </h6>
                    <div class="d-flex align-items-center mt-1">
                        <label class="small me-2 mb-0">Relationship Type:</label>
                        <select id="rel-type-${index}" class="form-select form-select-sm w-auto">
                            <option value="nested" ${rel.type === 'nested' ? 'selected' : ''}>nested</option>
                            <option value="parent-child" ${rel.type === 'parent-child' ? 'selected' : ''}>parent-child</option>
                        </select>
                    </div>
                </div>
                <span class="detection-confidence ${confidenceClass}">
                    ${(confidence * 100).toFixed(0)}% confidence
                </span>
            </div>
        `;

        const configDiv = document.createElement('div');
        configDiv.className = `mt-2 parent-child-config ${rel.type === 'parent-child' ? '' : 'd-none'}`;
        configDiv.innerHTML = `
            <div class="small text-muted mb-1">Configure Parent-Child Relationship</div>
            <div class="row g-2">
                <div class="col">
                    <input type="text" id="rel-name-${index}" class="form-control form-control-sm" placeholder="Relationship Name">
                </div>
                <div class="col">
                    <input type="text" id="parent-type-${index}" class="form-control form-control-sm" placeholder="Parent Type">
                </div>
                <div class="col">
                    <input type="text" id="child-type-${index}" class="form-control form-control-sm" placeholder="Child Type">
                </div>
            </div>
        `;
        resultDiv.appendChild(configDiv);

        const typeSelect = resultDiv.querySelector(`#rel-type-${index}`);
        typeSelect.addEventListener('change', (e) => {
            if (e.target.value === 'parent-child') {
                configDiv.classList.remove('d-none');
            } else {
                configDiv.classList.add('d-none');
            }
        });

        container.appendChild(resultDiv);
    });
}
function applyDetectionResults() {
    if (workflowData.detectionResults) {
        const container = document.getElementById('detectionResults');
        for (let idx = 0; idx < workflowData.detectionResults.length; idx++) {
            const rel = workflowData.detectionResults[idx];
            const select = container.querySelector(`#rel-type-${idx}`);
            if (select) {
                rel.type = select.value;

                if (rel.type === 'parent-child') {
                    const nameInput = container.querySelector(`#rel-name-${idx}`);
                    const parentInput = container.querySelector(`#parent-type-${idx}`);
                    const childInput = container.querySelector(`#child-type-${idx}`);

                    rel.relationName = nameInput ? nameInput.value.trim() : '';
                    rel.parentType = parentInput ? parentInput.value.trim() : '';
                    rel.childType = childInput ? childInput.value.trim() : '';

                    if (!rel.relationName || !rel.parentType || !rel.childType) {
                        showAlert('Please fill all parent-child relationship fields', 'warning');
                        return;
                    }
                } else {
                    delete rel.relationName;
                    delete rel.parentType;
                    delete rel.childType;
                }
            }
        }

        workflowData.relationships = [...workflowData.detectionResults];
        updateRelationshipsDisplay();

        // Close modal
        const modal = bootstrap.Modal.getInstance(document.getElementById('autoDetectionModal'));
        modal.hide();

        goToStep(4);
        completeStep(4);
        showAlert(`Applied ${workflowData.relationships.length} detected relationships`, 'success');
    }
}

function addRelationship() {
    console.log("🔧 Opening manual relationship modal");
    console.log("Available tables:", workflowData.selectedTables);
    console.log("Table structures:", workflowData.tableStructures);

    // Populate parent table options
    const parentSelect = document.getElementById('parentTable');
    const childSelect = document.getElementById('childTable');

    if (!parentSelect || !childSelect) {
        console.error("❌ Modal select elements not found");
        showAlert('Modal elements not found. Please check the HTML.', 'danger');
        return;
    }

    // Clear existing options
    parentSelect.innerHTML = '<option value="">Select parent table...</option>';
    childSelect.innerHTML = '<option value="">Select child table...</option>';

    // Add table options
    workflowData.selectedTables.forEach(table => {
        console.log(`Adding table option: ${table}`);

        const parentOption = document.createElement('option');
        parentOption.value = table;
        parentOption.textContent = table;
        parentSelect.appendChild(parentOption);

        const childOption = document.createElement('option');
        childOption.value = table;
        childOption.textContent = table;
        childSelect.appendChild(childOption);
    });

    // Clear field selectors
    const parentFieldSelect = document.getElementById('parentField');
    const childFieldSelect = document.getElementById('childField');

    if (parentFieldSelect) parentFieldSelect.innerHTML = '<option value="">Select parent field...</option>';
    if (childFieldSelect) childFieldSelect.innerHTML = '<option value="">Select child field...</option>';

    // Add event listeners for table selection changes
    setupTableFieldListeners();

    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('relationshipModal'));
    modal.show();
}


function setupTableFieldListeners() {
    const parentTableSelect = document.getElementById('parentTable');
    const childTableSelect = document.getElementById('childTable');

    if (parentTableSelect) {
        parentTableSelect.addEventListener('change', function() {
            console.log(`Parent table changed to: ${this.value}`);
            populateFieldOptions('parentField', this.value);
        });
    }

    if (childTableSelect) {
        childTableSelect.addEventListener('change', function() {
            console.log(`Child table changed to: ${this.value}`);
            populateFieldOptions('childField', this.value);
        });
    }
}

function populateFieldOptions(fieldSelectId, tableName) {
    const fieldSelect = document.getElementById(fieldSelectId);







    if (!fieldSelect || !tableName) {
        console.log(`No field select or table name: fieldSelect=${!!fieldSelect}, tableName=${tableName}`);
        return;
    }

    // Clear existing options
    fieldSelect.innerHTML = '<option value="">Loading fields...</option>';

    // Get table structure
    const tableColumns = workflowData.tableStructures[tableName];

    if (!tableColumns || tableColumns.length === 0) {
        console.error(`No columns found for table: ${tableName}`);
        fieldSelect.innerHTML = '<option value="">No fields found</option>';
        return;
    }

    console.log(`Populating ${fieldSelectId} with columns from ${tableName}:`, tableColumns);

    // Clear and add field options
    fieldSelect.innerHTML = '<option value="">Select field...</option>';

    tableColumns.forEach(column => {
        const fieldName = column.name || column.column_name || column.COLUMN_NAME;
        const fieldType = column.type || column.data_type || column.DATA_TYPE;

        if (fieldName) {
            const option = document.createElement('option');
            option.value = fieldName;
            option.textContent = `${fieldName} (${fieldType || 'unknown'})`;
            fieldSelect.appendChild(option);

            console.log(`Added field option: ${fieldName} (${fieldType})`);
        }
    });
}

function saveRelationship() {
    console.log("💾 Saving relationship...");

    // Get form values
    const parentTable = document.getElementById('parentTable').value;
    const parentField = document.getElementById('parentField').value;
    const childTable = document.getElementById('childTable').value;
    const childField = document.getElementById('childField').value;
    const relationshipTypeElement = document.querySelector('.relationship-type-option.selected');

    // Debug form values
    console.log("Form values:", {
        parentTable,
        parentField,
        childTable,
        childField,
        relationshipTypeElement: !!relationshipTypeElement
    });

    // Validation
    if (!parentTable || !parentField || !childTable || !childField) {
        const missingFields = [];
        if (!parentTable) missingFields.push('Parent Table');
        if (!parentField) missingFields.push('Parent Field');
        if (!childTable) missingFields.push('Child Table');
        if (!childField) missingFields.push('Child Field');

        showAlert(`Please fill in all relationship fields. Missing: ${missingFields.join(', ')}`, 'warning');
        return;
    }

    if (!relationshipTypeElement) {
        showAlert('Please select a relationship type', 'warning');
        return;
    }

    const relationType = relationshipTypeElement.dataset.type;

    // Create relationship object
    const relationship = {
        parentTable: parentTable,
        parentField: parentField,
        childTable: childTable,
        childField: childField,
        type: relationType,
        id: Date.now(),
        source: 'manual'
    };

    console.log("✅ Created relationship:", relationship);

    // Add to global data
    if (!workflowData.relationships) {
        workflowData.relationships = [];
    }

    workflowData.relationships.push(relationship);

    // Update display
    updateRelationshipsDisplay();

    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('relationshipModal'));
    if (modal) {
        modal.hide();
    }

    showAlert(`Relationship added: ${parentTable}.${parentField} → ${childTable}.${childField}`, 'success');
}


function removeRelationship(index) {
    workflowData.relationships.splice(index, 1);
    updateRelationshipsDisplay();
}

function generateFieldMappings() {
    const container = document.getElementById('fieldMappingContainer');

    if (!container) {
        console.error("❌ fieldMappingContainer not found");
        return;
    }

    const missingStructures = [];
    const allTables = new Set();

    workflowData.relationships.forEach(rel => {
        allTables.add(rel.parentTable);
        allTables.add(rel.childTable);
    });

    // Check if structures exist for all tables
    allTables.forEach(table => {
        if (!workflowData.tableStructures[table] || workflowData.tableStructures[table].length === 0) {
            missingStructures.push(table);
        }
    });

    if (missingStructures.length > 0) {
        console.log(`❌ Missing table structures for: ${missingStructures.join(', ')}`);
        container.innerHTML = `
            <div class="alert alert-warning">
                <h6><i class="fas fa-exclamation-triangle me-2"></i>Missing Table Structures</h6>
                <p>Table structures are missing for: <strong>${missingStructures.join(', ')}</strong></p>
                <button class="btn btn-primary" onclick="loadMissingTableStructures(['${missingStructures.join("','")}'])">
                    <i class="fas fa-download me-2"></i>Load Missing Structures
                </button>
                <button class="btn btn-secondary ms-2" onclick="goToStep(3)">
                    <i class="fas fa-arrow-left me-2"></i>Go Back to Step 3
                </button>
            </div>
        `;
        return;
    }


    console.log("🗺️ Generating field mappings for relationships:", workflowData.relationships);

    container.innerHTML = '';

    if (!workflowData.relationships || workflowData.relationships.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-5">
                <i class="fas fa-cogs fa-3x mb-3"></i>
                <p>Define relationships to configure field mappings</p>
            </div>
        `;
        return;
    }

    workflowData.relationships.forEach((rel, relIndex) => {
        // Validate relationship has all required fields
        if (!rel.parentTable || !rel.parentField || !rel.childTable || !rel.childField) {
            console.error(`❌ Skipping invalid relationship at index ${relIndex}:`, rel);
            return;
        }

        const mappingCard = document.createElement('div');
        mappingCard.className = 'card mb-3';
        mappingCard.innerHTML = `
            <div class="card-header d-flex justify-content-between align-items-center" data-bs-toggle="collapse" data-bs-target="#fieldMappingBody_${relIndex}" style="cursor: pointer;">
                <h6 class="mb-0">
                    <i class="fas fa-arrows-alt-h me-2"></i>
                    ${rel.parentTable} → ${rel.childTable} Field Mapping
                    <span class="badge bg-${rel.type === 'nested' ? 'success' : rel.type === 'parent-child' ? 'primary' : 'info'} ms-2">
                        ${rel.type}
                    </span>
                </h6>
                <i class="fas fa-chevron-down toggle-icon"></i>
            </div>
            <div id="fieldMappingBody_${relIndex}" class="collapse">
                <div class="card-body">
                    <div id="fieldMapping_${relIndex}">
                        <div class="text-center">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">Loading...</span>
                            </div>
                            <p class="mt-2">Generating field mappings...</p>
                        </div>
                    </div>
                </div>
            </div>
        `;
        container.appendChild(mappingCard);

        const collapseEl = mappingCard.querySelector(`#fieldMappingBody_${relIndex}`);
        const icon = mappingCard.querySelector('.toggle-icon');
        collapseEl.addEventListener('show.bs.collapse', () => {
            icon.classList.replace('fa-chevron-down', 'fa-chevron-up');
        });
        collapseEl.addEventListener('hide.bs.collapse', () => {
            icon.classList.replace('fa-chevron-up', 'fa-chevron-down');
        });

        // Generate the actual mapping content
        setTimeout(() => {
            generateRelationshipFieldMapping(rel, relIndex);
        }, 100);
    });

    // Move to step 5
    goToStep(5);
    completeStep(5);
    setTimeout(() => {
        enableStep6();
    }, 500);
}

function generateRelationshipFieldMapping(relationship, relIndex) {
    const container = document.getElementById(`fieldMapping_${relIndex}`);

    if (!container) {
        console.error(`❌ Field mapping container ${relIndex} not found`);
        return;
    }

    console.log(`🗺️ Generating enhanced field mapping for relationship ${relIndex}:`, relationship);

    // Ensure field mapping storage for this relationship
    if (!workflowData.fieldMappings[relIndex]) {
        workflowData.fieldMappings[relIndex] = { parent: {}, child: {} };
    }

    const parentFields = workflowData.tableStructures[relationship.parentTable] || [];
    const childFields = workflowData.tableStructures[relationship.childTable] || [];

    console.log(`Parent fields (${relationship.parentTable}):`, parentFields.length);
    console.log(`Child fields (${relationship.childTable}):`, childFields.length);

    if (parentFields.length === 0 && childFields.length === 0) {
        container.innerHTML = `
            <div class="alert alert-warning">
                <i class="fas fa-exclamation-triangle me-2"></i>
                No field structures found for these tables. Please load table structures first.
            </div>
        `;
        return;
    }

    // Generate mapping based on relationship type
    let mappingHTML = '';

    if (relationship.type === 'nested') {
        mappingHTML = generateNestedFieldMapping(relationship, parentFields, childFields, relIndex);
    } else if (relationship.type === 'parent-child' || relationship.type === 'join') {
        mappingHTML = generateJoinFieldMapping(relationship, parentFields, childFields, relIndex);
    } else {
        mappingHTML = generateObjectFieldMapping(relationship, parentFields, childFields, relIndex);
    }

    container.innerHTML = mappingHTML;
}

// 🆕 Generate nested field mapping (parent + nested child)
function generateNestedFieldMapping(relationship, parentFields, childFields, relIndex) {
    return `
        <!-- Parent Table Fields -->
        <div class="row mb-4">
            <div class="col-md-6">
                <h6 class="text-danger">
                    <i class="fas fa-database me-2"></i>
                    Oracle Parent Table (${relationship.parentTable})
                </h6>
                <div class="border rounded p-3" style="max-height: 300px; overflow-y: auto; background: #fff5f5;">
                    ${generateOracleFieldsHTML(parentFields)}
                </div>
            </div>
            <div class="col-md-6">
                <h6 class="text-primary">
                    <i class="fas fa-search me-2"></i>
                    Elasticsearch Parent Mapping
                </h6>
                <div class="border rounded p-3" style="max-height: 300px; overflow-y: auto; background: #f0f8ff;">
                    ${generateElasticsearchFieldsHTML(parentFields, relIndex, 'parent')}
                </div>
            </div>
        </div>

        <!-- Child Table Fields (Nested) -->
        <div class="row mb-4">
            <div class="col-md-6">
                <h6 class="text-warning">
                    <i class="fas fa-layer-group me-2"></i>
                    Oracle Child Table (${relationship.childTable}) - Will be nested
                </h6>
                <div class="border rounded p-3" style="max-height: 300px; overflow-y: auto; background: #fffbf0;">
                    ${generateOracleFieldsHTML(childFields)}
                </div>
            </div>
            <div class="col-md-6">
                <h6 class="text-success">
                    <i class="fas fa-code-branch me-2"></i>
                    Elasticsearch Nested Mapping
                </h6>
                <div class="border rounded p-3" style="max-height: 300px; overflow-y: auto; background: #f0fff0;">
                    <div class="nested-field-container">
                        <div class="elastic-field mb-2" style="border-left: 4px solid #28a745;">
                            <strong>${relationship.childTable.toLowerCase()}_items</strong>
                            <br><small class="text-muted">nested</small>
                            <div class="ms-3 mt-2 p-2" style="border-left: 2px dashed #28a745;">
                                ${generateNestedElasticsearchFieldsHTML(childFields, relIndex)}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Relationship Summary -->
        <div class="row">
            <div class="col-12">
                <div class="alert alert-info">
                    <h6><i class="fas fa-info-circle me-2"></i>Nested Relationship Mapping</h6>
                    <div class="row">
                        <div class="col-md-6">
                            <strong>Join Condition:</strong> ${relationship.parentTable}.${relationship.parentField} = ${relationship.childTable}.${relationship.childField}
                        </div>
                        <div class="col-md-6">
                            <strong>Structure:</strong> Child records nested within parent documents
                        </div>
                    </div>
                    <hr class="my-2">
                    <small class="text-muted">
                        <i class="fas fa-lightbulb me-1"></i>
                        Each ${relationship.parentTable} document will contain an array of ${relationship.childTable.toLowerCase()}_items with all related child records.
                    </small>
                </div>
            </div>
        </div>

        <!-- Elasticsearch Mapping Preview -->
        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0">
                            <i class="fas fa-code me-2"></i>
                            Generated Elasticsearch Mapping Preview
                        </h6>
                    </div>
                    <div class="card-body">
                        <pre class="small bg-light p-3 rounded"><code>${generateMappingPreview(relationship, parentFields, childFields, relIndex)}</code></pre>
                    </div>
                </div>
            </div>
        </div>
    `;
}

async function generateWorkflowMapping(isSave = false) {
    const mappingName = document.getElementById('workflowMappingName').value.trim();
    const indexName = document.getElementById('workflowIndexName').value.trim();
    const elasticEnvId = document.getElementById('workflowElasticEnvironment').value;
    const buttonId = isSave ? 'saveWorkflowMappingBtn' : 'generateWorkflowMappingBtn';
    const autoDetectionBtn = document.getElementById('runAutoDetection').value;

    if (!mappingName || !indexName || !elasticEnvId) {
        showAlert('Please enter mapping name, index name, and select an Elasticsearch environment', 'warning');
        return;
    }

    if (!workflowData.selectedTables || workflowData.selectedTables.length === 0) {
        showAlert('No tables selected. Please complete the workflow steps.', 'error');
        return;
    }

    if (!workflowData.relationships || workflowData.relationships.length === 0) {
        showAlert('No relationships defined. Please define at least one relationship.', 'error');
        return;
    }

    try {
        showLoading(buttonId);

        const mappingData = {
            mappingName: mappingName,
            indexName: indexName,
            tables: workflowData.selectedTables,
            relationships: workflowData.relationships,
            tableStructures: workflowData.tableStructures,
            fieldMappings: workflowData.fieldMappings,
            autoDetectionBtn:autoDetectionBtn
        };

        const response = await fetch(`/oracle/generate-workflow-mapping/${elasticEnvId}?dry_run=${!isSave}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(mappingData)
        });

        const result = await response.json();

        if (result.success) {
            displayMappingSummary(result);
            updateMappingPreview(result.mapping);
            workflowGeneratedMapping = result.mapping;
            if (isSave) {
                completeStep(6);
                showAlert('Mapping saved successfully!', 'success');
                showWorkflowCompletion(result);
            } else {
                showAlert('Mapping generated successfully!', 'success');
            }
        } else {
            throw new Error(result.error);
        }

    } catch (error) {
        showAlert('Error generating mapping: ' + error.message, 'danger');
    } finally {
        hideLoading(buttonId);
        updateGenerateWorkflowButton();
    }
}




function displayFinalMappingSummary(result) {
    const container = document.getElementById('mappingSummaryContent');

    const finalSummary = `
        <div class="alert alert-success">
            <h5><i class="fas fa-check-circle me-2"></i>Mapping Generated Successfully!</h5>
            <div class="row">
                <div class="col-md-6">
                    <strong>Mapping Name:</strong> ${result.mappingName || 'N/A'}<br>
                    <strong>Index Name:</strong> ${result.indexName || 'N/A'}<br>
                    <strong>Tables Processed:</strong> ${workflowData.selectedTables.length}
                </div>
                <div class="col-md-6">
                    <strong>Total Fields:</strong> ${result.totalFields || calculateTotalFields()}<br>
                    <strong>Relationships:</strong> ${workflowData.relationships.length}<br>
                    <strong>Status:</strong> <span class="badge bg-success">Ready</span>
                </div>
            </div>
        </div>
        
        ${container.innerHTML}
    `;

    container.innerHTML = finalSummary;
}

// 🆕 NEW FUNCTION: Update mapping preview
function updateMappingPreview(mapping) {
    const container = document.getElementById('workflowPreviewContent');

    if (container && mapping) {
        container.textContent = JSON.stringify(mapping, null, 2);
    }
}

function showWorkflowMappingPreview() {
    if (!workflowGeneratedMapping) {
        showAlert('Please generate the mapping first', 'warning');
        return;
    }
    const modalEl = document.getElementById('workflowPreviewModal');
    if (modalEl) {
        const modal = new bootstrap.Modal(modalEl);
        modal.show();
    }
}

function showFieldDataMappingModal() {
    populateFieldDataMappingModal();
    const modalEl = document.getElementById('fieldDataMappingModal');
    if (modalEl) {
        const modal = new bootstrap.Modal(modalEl);
        modal.show();
    }
}

function getAllEsFields(properties, prefix = '') {
    const fields = [];
    Object.entries(properties || {}).forEach(([key, value]) => {
        const path = prefix ? `${prefix}.${key}` : key;
        fields.push(path);
        if (value.properties) {
            fields.push(...getAllEsFields(value.properties, path));
        }
        if (value.fields) {
            fields.push(...getAllEsFields(value.fields, path));
        }
    });
    return fields;
}

function populateFieldDataMappingModal() {
    const tbody = document.getElementById('fieldDataMappingBody');
    if (!tbody) return;

    tbody.innerHTML = '';
    const columnSet = new Set();
    Object.values(workflowData.tableStructures || {}).forEach(cols => {
        cols.forEach(c => columnSet.add(c.name || c));
    });
    const columns = Array.from(columnSet);

    const esFields = workflowGeneratedMapping?.mappings?.properties
        ? getAllEsFields(workflowGeneratedMapping.mappings.properties)
        : [];

    if (columns.length === 0 || esFields.length === 0) {
        tbody.innerHTML = '<tr><td colspan="2" class="text-center text-muted">No data available</td></tr>';
        return;
    }

    const existingMapping = workflowData.columnFieldMapping || {};

    columns.forEach(col => {
        const mappedField = existingMapping[col] || (esFields.includes(col) ? col : '');
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${col}</td>
            <td>
                <div class="d-flex align-items-center gap-2">
                    <span class="es-field-text">${mappedField}</span>
                    <select class="form-select form-select-sm field-mapping-select d-none">
                        <option value="">Select field...</option>
                        ${esFields.map(f => `<option value="${f}" ${f === mappedField ? 'selected' : ''}>${f}</option>`).join('')}
                    </select>
                    <button type="button" class="btn btn-sm btn-outline-secondary edit-mapping-btn">
                        <i class="fas fa-edit"></i>
                    </button>
                </div>
            </td>
        `;
        const editBtn = tr.querySelector('.edit-mapping-btn');
        const select = tr.querySelector('.field-mapping-select');
        const textSpan = tr.querySelector('.es-field-text');
        editBtn.addEventListener('click', () => {
            textSpan.classList.add('d-none');
            select.classList.remove('d-none');
            select.focus();
            if (select.showPicker) {
                select.showPicker();
            }
        });
        select.addEventListener('change', () => {
            textSpan.textContent = select.value;
            select.classList.add('d-none');
            textSpan.classList.remove('d-none');
        });
        tbody.appendChild(tr);
    });
}

function saveFieldDataMapping() {
    const rows = document.querySelectorAll('#fieldDataMappingBody tr');
    const mapping = {};
    rows.forEach(row => {
        const oracleCol = row.cells[0]?.textContent.trim();
        const select = row.querySelector('.field-mapping-select');
        const textSpan = row.querySelector('.es-field-text');
        let esField = '';
        if (select && !select.classList.contains('d-none')) {
            esField = select.value;
        } else if (textSpan) {
            esField = textSpan.textContent.trim();
        }
        if (oracleCol && esField) {
            mapping[oracleCol] = esField;
        }
    });
    workflowData.columnFieldMapping = mapping;
    showAlert('Field data mapping saved', 'success');
    const modalEl = document.getElementById('fieldDataMappingModal');
    if (modalEl) {
        const modal = bootstrap.Modal.getInstance(modalEl);
        if (modal) modal.hide();
    }
}

async function handleDataLoadElasticEnvChange() {
    const envId = document.getElementById('dataLoadElasticEnv')?.value;

    const indexSelect = document.getElementById('dataLoadElasticsearchIndex');
    if (!indexSelect) return;
    indexSelect.innerHTML = '<option value="">Select index...</option>';
    if (!envId) return;
    try {
        if (envId === null) {
            const resp = await fetch(`/indices/${envId}`);
            const indices = await resp.json();
            indices.forEach(idx => {
                const option = document.createElement('option');
                option.value = idx.index;
                option.textContent = idx.index;
                indexSelect.appendChild(option);
            });
        }else
        {
            const envIdTemp = (String(envId ?? '').split('-', 2)[1] ?? '');
            const resp = await fetch(`/indices/${envIdTemp}`);
            const indices = await resp.json();
            indices.forEach(idx => {
                const option = document.createElement('option');
                option.value = idx.index;
                option.textContent = idx.index;
                indexSelect.appendChild(option);
            });
        }

    } catch (err) {
        console.error('Error loading Elasticsearch indices:', err);
        showAlert('Error loading Elasticsearch indices: ' + err.message, 'danger');
    }
}

async function showDataLoadModal() {
    try {
        // Refresh environments and populate dropdowns
        await loadEnvironments();

        // Preselect workflow Oracle environment if available
        const oracleSelect = document.getElementById('dataLoadOracleEnv');
        if (oracleSelect && workflowData.selectedEnvironment) {
            oracleSelect.value = workflowData.selectedEnvironment;
        }

        // Preselect workflow Elasticsearch environment and load indices
        const esEnvSelect = document.getElementById('dataLoadElasticEnv');
        if (esEnvSelect) {
            const workflowEnv = document.getElementById('workflowElasticEnvironment')?.value;
            if (workflowEnv) {
                esEnvSelect.value = workflowEnv;
            }
            await handleDataLoadElasticEnvChange();
        }

        const modalEl = document.getElementById('dataLoadModal');
        if (modalEl) {
            const modal = new bootstrap.Modal(modalEl);
            modal.show();
        }
    } catch (error) {
        console.error('Error showing Data Load modal:', error);
        showAlert('Error preparing Data Load modal: ' + error.message, 'danger');
    }
}

async function runDataLoad() {
    const oracleEnvId = document.getElementById('dataLoadOracleEnv')?.value;
    const elasticEnvId = document.getElementById('dataLoadElasticEnv')?.value;
    const indexName = document.getElementById('dataLoadElasticsearchIndex')?.value;
    const query = document.getElementById('dataLoadQuery')?.value.trim();

    if (!oracleEnvId) {
        showAlert('Please select an Oracle environment', 'warning');
        return;
    }
    if (!elasticEnvId) {
        showAlert('Please select an Elasticsearch environment', 'warning');
        return;
    }
    if (!indexName) {
        showAlert('Please select an Elasticsearch index', 'warning');
        return;
    }


    try {
        showAlert('Starting data load...', 'info');
        console.log(oracleEnvId);
        console.log(elasticEnvId);
        const envIdTemp = (String(elasticEnvId ?? '').split('-', 2)[1] ?? '');
        const formData = new FormData();
        formData.append('oracle_env_id', oracleEnvId);
        formData.append('elastic_env_id', envIdTemp);
        formData.append('index', indexName);
        formData.append('query', query);
        const response = await fetch('/oracle/data-load', {
            method: 'POST',
            body: formData
        });
        const result = await response.json();
        if (result.success) {
            showAlert(`Loaded ${result.indexed} records into ${indexName}`, 'success');
        } else {
            showAlert('Data load failed: ' + (result.error || 'Unknown error'), 'danger');
        }
        const modalEl = document.getElementById('dataLoadModal');
        if (modalEl) {
            const modal = bootstrap.Modal.getInstance(modalEl);
            if (modal) modal.hide();
        }
    } catch (error) {
        showAlert('Data load failed: ' + error.message, 'danger');
    }
}

// 🆕 NEW FUNCTION: Show workflow completion
function showWorkflowCompletion(result) {
    const completionModal = `
        <div class="modal fade" id="workflowCompletionModal" tabindex="-1">
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header bg-success text-white">
                        <h5 class="modal-title">
                            <i class="fas fa-check-circle me-2"></i>Workflow Completed Successfully!
                        </h5>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="text-center mb-4">
                            <i class="fas fa-trophy fa-4x text-warning mb-3"></i>
                            <h4>Congratulations!</h4>
                            <p class="lead">Your Oracle to Elasticsearch mapping has been generated successfully.</p>
                        </div>
                        
                        <div class="row">
                            <div class="col-md-6">
                                <h6>📋 Summary</h6>
                                <ul class="list-unstyled">
                                    <li><strong>Tables:</strong> ${workflowData.selectedTables.length}</li>
                                    <li><strong>Relationships:</strong> ${workflowData.relationships.length}</li>
                                    <li><strong>Total Fields:</strong> ${result.totalFields || calculateTotalFields()}</li>
                                    <li><strong>Index:</strong> ${result.indexName || 'N/A'}</li>
                                </ul>
                            </div>
                            <div class="col-md-6">
                                <h6>🎯 Next Steps</h6>
                                <ul class="list-unstyled">
                                    <li>✅ Mapping is ready to use</li>
                                    <li>📤 Export mapping if needed</li>
                                    <li>🔄 Set up data sync process</li>
                                    <li>📊 Monitor data migration</li>
                                </ul>
                            </div>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-primary" onclick="exportMapping()">
                            <i class="fas fa-download me-1"></i>Export Mapping
                        </button>
                        <button type="button" class="btn btn-success" onclick="startNewWorkflow()">
                            <i class="fas fa-plus me-1"></i>Start New Workflow
                        </button>
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `;

    // Remove existing modal if any
    const existingModal = document.getElementById('workflowCompletionModal');
    if (existingModal) {
        existingModal.remove();
    }

    // Add and show modal
    document.body.insertAdjacentHTML('beforeend', completionModal);
    const modal = new bootstrap.Modal(document.getElementById('workflowCompletionModal'));
    modal.show();
}

// 🆕 NEW FUNCTION: Export mapping
function exportMapping() {
    const mappingData = {
        name: document.getElementById('workflowMappingName').value,
        index: document.getElementById('workflowIndexName').value,
        tables: workflowData.selectedTables,
        relationships: workflowData.relationships,
        mapping: JSON.parse(document.getElementById('workflowPreviewContent').textContent || '{}')
    };

    const blob = new Blob([JSON.stringify(mappingData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = `${mappingData.name || 'oracle_mapping'}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    showAlert('Mapping exported successfully!', 'success');
}

// 🆕 NEW FUNCTION: Start new workflow
function startNewWorkflow() {
    // Reset workflow data
    workflowData = {
        currentStep: 1,
        selectedEnvironment: null,
        selectedTables: [],
        tableStructures: {},
        relationships: [],
        fieldMappings: {},
        detectionResults: null
    };

    // Reset UI
    goToStep(1);
    updateStepVisibility();

    // Clear form fields
    const forms = document.querySelectorAll('#oracle-env-connection input, #oracle-env-connection select, #oracle-env-connection textarea');
    forms.forEach(field => {
        if (field.type !== 'button') {
            field.value = '';
        }
    });

    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('workflowCompletionModal'));
    if (modal) {
        modal.hide();
    }

    showAlert('Started new workflow. You can begin with Step 1.', 'info');
}

function forceEnableStep6() {
    console.log("🔧 Force enabling Step 6 for testing");
    enableStep6();
}


function updateRelationshipsDisplay() {
    const container = document.getElementById('relationshipsContainer');

    if (!container) {
        console.error("❌ relationshipsContainer not found");
        return;
    }

    console.log("🔄 Updating relationships display. Current relationships:", workflowData.relationships);

    if (!workflowData.relationships || workflowData.relationships.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-4">
                <i class="fas fa-link fa-2x mb-3"></i>
                <p>Use auto-detection or manually add relationships</p>
            </div>
        `;
        return;
    }

    container.innerHTML = '';

    workflowData.relationships.forEach((rel, index) => {
        // Validate relationship object
        if (!rel.parentTable || !rel.parentField || !rel.childTable || !rel.childField) {
            console.error(`❌ Invalid relationship at index ${index}:`, rel);
            return;
        }

        const relDiv = document.createElement('div');
        relDiv.className = 'relationship-item';
        relDiv.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <h6 class="mb-1">
                        <i class="fas fa-${getRelationshipIcon(rel.type)} me-2"></i>
                        ${rel.parentTable}.${rel.parentField} → ${rel.childTable}.${rel.childField}
                    </h6>
                    <div>
                        <span class="badge bg-warning">${rel.type}</span>
                        ${rel.source ? `<span class="badge bg-info ms-1">${rel.source}</span>` : ''}
                        ${rel.confidence ? `<span class="badge bg-secondary ms-1">${Math.round(rel.confidence * 100)}%</span>` : ''}
                    </div>
                </div>
                <button class="btn btn-sm btn-outline-danger" onclick="removeRelationship(${index})">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        `;
        container.appendChild(relDiv);
    });

    // Auto-proceed to next step if we have relationships
    if (workflowData.relationships.length > 0) {
        console.log("✅ Relationships exist, generating field mappings");
        generateFieldMappings();
    }
}


function displayMappingSummary(result) {
    const container = document.getElementById('mappingSummaryContent');
    container.innerHTML = `
        <div class="row">
            <div class="col-md-6">
                <h6><i class="fas fa-table me-2"></i>Tables Processed</h6>
                <ul class="list-unstyled">
                    ${workflowData.selectedTables.map(table =>
        `<li><i class="fas fa-check text-success me-2"></i>${table}</li>`
    ).join('')}
                </ul>
            </div>
            <div class="col-md-6">
                <h6><i class="fas fa-link me-2"></i>Relationships</h6>
                <ul class="list-unstyled">
                    ${workflowData.relationships.map(rel =>
        `<li><i class="fas fa-arrow-right text-primary me-2"></i>${rel.parentTable} → ${rel.childTable}</li>`
    ).join('')}
                </ul>
            </div>
        </div>
        <div class="row mt-3">
            <div class="col-md-12">
                <div class="alert alert-success">
                    <i class="fas fa-check-circle me-2"></i>
                    Successfully generated mapping for <strong>${result.totalFields || 0}</strong> fields 
                    across <strong>${workflowData.selectedTables.length}</strong> tables with 
                    <strong>${workflowData.relationships.length}</strong> relationships.
                </div>
            </div>
        </div>
    `;
}
// Navigation functions
function goToStep(stepNumber) {
    // Update current step
    workflowData.currentStep = stepNumber;

    // Update progress indicator
    updateProgressIndicator(stepNumber);

    // Update step visibility
    updateStepVisibility();
}

function updateProgressIndicator(currentStep) {
    for (let i = 1; i <= 6; i++) {
        const stepElement = document.getElementById(`step-${['connect', 'select', 'structure', 'relationships', 'configure', 'generate'][i-1]}`);
        stepElement.classList.remove('active', 'completed');

        if (i === currentStep) {
            stepElement.classList.add('active');
        } else if (i < currentStep) {
            stepElement.classList.add('completed');
        }
    }
}

function updateStepVisibility() {
    for (let i = 1; i <= 6; i++) {
        const stepElement = document.getElementById(`workflow-step-${i}`);
        stepElement.classList.remove('active', 'completed');

        if (i === workflowData.currentStep) {
            stepElement.classList.add('active');
        } else if (i < workflowData.currentStep) {
            stepElement.classList.add('completed');
        }
    }
}

function completeStep(stepNumber) {
    const stepElement = document.getElementById(`workflow-step-${stepNumber}`);
    stepElement.classList.add('completed');

    // Enable next step if available
    if (stepNumber < 6) {
        goToStep(stepNumber + 1);
    }
}


function getRelationshipIcon(type) {
    const icons = {
        'nested': 'layer-group',
        'join': 'link',
        'object': 'cube',
        'parent-child': 'sitemap'
    };
    return icons[type] || 'link';
}


function generateOracleFieldsHTML(fields) {
    if (!fields || fields.length === 0) {
        return '<p class="text-muted">No fields available</p>';
    }

    return fields.map(field => {
        const fieldName = field.name || field.column_name || field.COLUMN_NAME || 'Unknown';
        const fieldType = field.type || field.data_type || field.DATA_TYPE || 'Unknown';

        return `
            <div class="oracle-field mb-2">
                <strong>${fieldName}</strong>
                <br><small class="text-muted">${fieldType}</small>
            </div>
        `;
    }).join('');
}

// 🔧 FIXED removeRelationship function
function removeRelationship(index) {
    console.log(`🗑️ Removing relationship at index ${index}`);

    if (!workflowData.relationships || index < 0 || index >= workflowData.relationships.length) {
        console.error(`❌ Invalid relationship index: ${index}`);
        return;
    }

    const removedRel = workflowData.relationships[index];
    workflowData.relationships.splice(index, 1);

    // 🔄 Reindex field type overrides so mapping generation uses current relationship order
    if (workflowData.fieldMappings && typeof workflowData.fieldMappings === 'object') {
        delete workflowData.fieldMappings[index];
        const reindexed = {};
        Object.keys(workflowData.fieldMappings)
            .sort((a, b) => parseInt(a) - parseInt(b))
            .forEach((key, i) => {
                reindexed[i] = workflowData.fieldMappings[key];
            });
        workflowData.fieldMappings = reindexed;
    }

    console.log(`✅ Removed relationship: ${removedRel.parentTable}.${removedRel.parentField} → ${removedRel.childTable}.${removedRel.childField}`);

    updateRelationshipsDisplay();
    showAlert('Relationship removed', 'info');
}


function generateJoinFieldMapping(relationship, parentFields, childFields, relIndex) {
    const joinFieldName = relationship.relationName || 'join_field';
    const parentType = (relationship.parentType || relationship.parentTable || '').toLowerCase();
    const childType = (relationship.childType || relationship.childTable || '').toLowerCase();

    return `
        <div class="row">
            <div class="col-md-6">
                <h6 class="text-danger">
                    <i class="fas fa-database me-2"></i>
                    Oracle Tables (${relationship.parentTable} + ${relationship.childTable})
                </h6>
                <div class="border rounded p-3" style="max-height: 400px; overflow-y: auto;">
                    <div class="mb-3">
                        <strong class="text-primary">${relationship.parentTable} (Parent):</strong>
                        ${generateOracleFieldsHTML(parentFields)}
                    </div>
                    <hr>
                    <div>
                        <strong class="text-warning">${relationship.childTable} (Child):</strong>
                        ${generateOracleFieldsHTML(childFields)}
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <h6 class="text-primary">
                    <i class="fas fa-search me-2"></i>
                    Elasticsearch Join Mapping
                </h6>
                <div class="border rounded p-3" style="max-height: 400px; overflow-y: auto;">
                    <div class="elastic-field mb-2">
                        <strong>${joinFieldName}</strong>
                        <br><small class="text-muted">join (parent: ${parentType}, child: ${childType})</small>
                    </div>
                    <hr class="my-2">
                    <div class="mb-2">
                        <strong class="text-primary">Parent fields:</strong>
                        ${generateElasticsearchFieldsHTML(parentFields, relIndex, 'parent')}
                    </div>
                    <div>
                        <strong class="text-warning">Child fields:</strong>
                        ${generateElasticsearchFieldsHTML(childFields, relIndex, 'child')}
                    </div>
                </div>
            </div>
        </div>

        <div class="row mt-3">
            <div class="col-12">
                <div class="alert alert-primary">
                    <i class="fas fa-link me-2"></i>
                    <strong>Parent-Child Join Relationship:</strong> ${relationship.parentTable}.${relationship.parentField} → ${relationship.childTable}.${relationship.childField}
                    <br>
                    <small>Documents will be indexed separately with join field indicating parent-child relationships.</small>
                </div>
            </div>
        </div>

        <div class="row mt-3">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0">
                            <i class="fas fa-code me-2"></i>
                            Generated Elasticsearch Mapping Preview
                        </h6>
                    </div>
                    <div class="card-body">
                        <pre class="small bg-light p-3 rounded"><code>${generateMappingPreview(relationship, parentFields, childFields, relIndex)}</code></pre>
                    </div>
                </div>
            </div>
        </div>
    `;
}

function generateObjectFieldMapping(relationship, parentFields, childFields, relIndex) {
    return `
        <div class="row">
            <div class="col-md-6">
                <h6 class="text-danger">
                    <i class="fas fa-database me-2"></i>
                    Oracle Fields (Combined)
                </h6>
                <div class="border rounded p-3" style="max-height: 300px; overflow-y: auto;">
                    ${generateOracleFieldsHTML([...parentFields, ...childFields])}
                </div>
            </div>
            <div class="col-md-6">
                <h6 class="text-primary">
                    <i class="fas fa-search me-2"></i>
                    Elasticsearch Object Mapping
                </h6>
                <div class="border rounded p-3" style="max-height: 300px; overflow-y: auto;">
                    ${generateElasticsearchFieldsHTML([...parentFields, ...childFields], relIndex, 'parent')}
                </div>
            </div>
        </div>
        
        <div class="row mt-3">
            <div class="col-12">
                <div class="alert alert-secondary">
                    <i class="fas fa-cube me-2"></i>
                    <strong>Object Relationship:</strong> Fields from both tables will be flattened into a single document structure.
                </div>
            </div>
        </div>
    `;
}


function generateNestedElasticsearchFieldsHTML(fields, relIndex) {
    if (!fields || fields.length === 0) {
        return '<p class="text-muted">No nested fields available</p>';
    }

    return fields.map(field => {
        const fieldName = getFieldName(field);
        const oracleType = getFieldType(field);
        let elasticType = oracleToElasticType(oracleType);
        const override = workflowData.fieldMappings[relIndex]?.child?.[fieldName.toLowerCase()];
        if (override) elasticType = override;

        return `
            <div class="nested-field-item mb-1 d-flex justify-content-between align-items-center">
                <div>
                    <small><strong>${fieldName.toLowerCase()}</strong></small>
                    <br><small class="text-muted">${elasticType}</small>
                </div>
                <button class="btn btn-sm btn-outline-secondary" onclick="openFieldEditModal(${relIndex}, 'child', '${fieldName.toLowerCase()}', '${elasticType}')"><i class="fas fa-edit"></i></button>
            </div>
        `;
    }).join('');
}


function generateMappingPreview(relationship, parentFields, childFields, relIndex) {
    const mapping = { "properties": {} };
    const parentOverrides = workflowData.fieldMappings[relIndex]?.parent || {};
    const childOverrides = workflowData.fieldMappings[relIndex]?.child || {};

    parentFields.forEach(field => {
        const fieldName = getFieldName(field);
        if (!fieldName) return;
        let elasticType = oracleToElasticType(getFieldType(field));
        const override = parentOverrides[fieldName.toLowerCase()];
        if (override) elasticType = override;
        mapping.properties[fieldName.toLowerCase()] = { type: elasticType };
    });

    if (relationship.type === 'nested') {
        const nestedProps = {};
        childFields.forEach(field => {
            const fieldName = getFieldName(field);
            if (!fieldName) return;
            let elasticType = oracleToElasticType(getFieldType(field));
            const override = childOverrides[fieldName.toLowerCase()];
            if (override) elasticType = override;
            nestedProps[fieldName.toLowerCase()] = { type: elasticType };
        });
        mapping.properties[`${relationship.childTable.toLowerCase()}_items`] = { type: 'nested', properties: nestedProps };
    } else {
        childFields.forEach(field => {
            const fieldName = getFieldName(field);
            if (!fieldName) return;
            let elasticType = oracleToElasticType(getFieldType(field));
            const override = childOverrides[fieldName.toLowerCase()];
            if (override) elasticType = override;
            mapping.properties[fieldName.toLowerCase()] = { type: elasticType };
        });
        if (relationship.type === 'parent-child' || relationship.type === 'join') {
            const joinFieldName = relationship.relationName || 'join_field';
            const parentType = (relationship.parentType || relationship.parentTable || '').toLowerCase();
            const childType = (relationship.childType || relationship.childTable || '').toLowerCase();
            mapping.properties[joinFieldName] = { type: 'join', relations: { [parentType]: childType } };
        }
    }

    return JSON.stringify(mapping, null, 2);
}

function openFieldEditModal(relIndex, scope, fieldName, currentType) {
    currentFieldEdit = { relIndex, scope, fieldName };
    const modalEl = document.getElementById('fieldTypeModal');
    const modal = new bootstrap.Modal(modalEl);
    document.getElementById('fieldTypeModalLabel').textContent = `Edit ${fieldName} Type`;
    const select = document.getElementById('fieldTypeSelect');
    if (select) select.value = currentType;
    modal.show();
}

function updateFieldType() {
    const select = document.getElementById('fieldTypeSelect');
    if (!currentFieldEdit || !select) return;
    const newType = select.value;
    const { relIndex, scope, fieldName } = currentFieldEdit;
    if (!workflowData.fieldMappings[relIndex]) {
        workflowData.fieldMappings[relIndex] = { parent: {}, child: {} };
    }
    // Store overrides in lowercase to match server-side mapping logic
    workflowData.fieldMappings[relIndex][scope][fieldName.toLowerCase()] = newType;
    const modalEl = document.getElementById('fieldTypeModal');
    const modal = bootstrap.Modal.getInstance(modalEl);
    if (modal) modal.hide();
    generateRelationshipFieldMapping(workflowData.relationships[relIndex], relIndex);
}

// 🔧 ENHANCED generateElasticsearchFieldsHTML with editing support
function generateElasticsearchFieldsHTML(fields, relIndex = null, scope = 'parent') {
    if (!fields || fields.length === 0) {
        return '<p class="text-muted">No fields available</p>';
    }

    return fields.map(field => {
        const fieldName = getFieldName(field);
        const oracleType = getFieldType(field);
        let elasticType = oracleToElasticType(oracleType);

        if (relIndex !== null) {
            const override = workflowData.fieldMappings[relIndex]?.[scope]?.[fieldName.toLowerCase()];
            if (override) {
                elasticType = override;
            }
        }

        const typeColor = getTypeColor(elasticType);
        const editBtn = relIndex !== null ? `<button class="btn btn-sm btn-outline-secondary ms-2" onclick="openFieldEditModal(${relIndex}, '${scope}', '${fieldName.toLowerCase()}', '${elasticType}')"><i class="fas fa-edit"></i></button>` : '';

        return `
            <div class="elastic-field mb-2 d-flex justify-content-between align-items-center">
                <div>
                    <strong>${fieldName.toLowerCase()}</strong>
                    <br><small class="text-muted">
                        <span class="badge" style="background-color: ${typeColor}; color: white; font-size: 10px;">${elasticType}</span>
                        <span class="text-muted ms-1">(was ${oracleType})</span>
                    </small>
                </div>
                ${editBtn}
            </div>
        `;
    }).join('');
}

// 🆕 Get color for Elasticsearch data types
function getTypeColor(elasticType) {
    const colors = {
        'text': '#17a2b8',
        'keyword': '#6f42c1',
        'long': '#28a745',
        'integer': '#28a745',
        'double': '#fd7e14',
        'float': '#fd7e14',
        'date': '#dc3545',
        'boolean': '#6c757d',
        'binary': '#343a40',
        'nested': '#20c997'
    };
    return colors[elasticType] || '#6c757d';
}

function getFieldName(column) {
    // Try different possible property names for field name
    return column.name ||
        column.column_name ||
        column.COLUMN_NAME ||
        column.columnName ||
        column.field_name ||
        column.FIELD_NAME ||
        (typeof column === 'string' ? column : null);
}

function getFieldType(column) {
    // Try different possible property names for field type
    if (typeof column === 'string') return 'unknown';

    return column.type ||
        column.data_type ||
        column.DATA_TYPE ||
        column.dataType ||
        column.field_type ||
        column.FIELD_TYPE ||
        'unknown';
}

function updateGenerateWorkflowButton() {
    const mappingName = document.getElementById('workflowMappingName')?.value.trim();
    const indexName = document.getElementById('workflowIndexName')?.value.trim();
    const envId = document.getElementById('workflowElasticEnvironment')?.value;
    const generateBtn = document.getElementById('generateWorkflowMappingBtn');
    const saveBtn = document.getElementById('saveWorkflowMappingBtn');
    const previewBtn = document.getElementById('previewWorkflowMappingBtn');
    const fieldDataMappingBtn = document.getElementById('fieldDataMappingBtn');
    const dataLoadBtn = document.getElementById('dataLoadBtn');

    if (generateBtn) {
        generateBtn.disabled = !(mappingName && indexName && envId);
    }

    const hasMapping = !!workflowGeneratedMapping;
    if (saveBtn) {
        saveBtn.disabled = !hasMapping;
    }
    if (previewBtn) {
        previewBtn.disabled = !hasMapping;
    }
    if (fieldDataMappingBtn) {
        fieldDataMappingBtn.disabled = !hasMapping;
    }
    if (dataLoadBtn) {
        dataLoadBtn.disabled = !hasMapping;
    }
    if (!hasMapping) {
        const container = document.getElementById('workflowPreviewContent');
        if (container) {
            container.textContent = 'No mapping generated.';
        }
    }
}

function enableStep6() {
    // Move to Step 6
    goToStep(6);

    // Populate mapping summary
    populateMappingSummary();

    // Enable the generate button based on field completion
    const generateBtn = document.getElementById('generateWorkflowMappingBtn');
    if (generateBtn) {
        updateGenerateWorkflowButton();
    }

    // Auto-populate field names if empty
    autoPopulateConfigFields();
}

function populateMappingSummary() {
    const container = document.getElementById('mappingSummaryContent');

    if (!container) {
        console.error("❌ mappingSummaryContent not found");
        return;
    }

    console.log("📊 Populating mapping summary");

    // Calculate summary statistics
    const totalTables = workflowData.selectedTables.length;
    const totalRelationships = workflowData.relationships.length;
    const totalFields = calculateTotalFields();
    const relationshipTypes = getRelationshipTypes();

    container.innerHTML = `
        <div class="row">
            <div class="col-md-6">
                <h6><i class="fas fa-table me-2 text-danger"></i>Tables Selected (${totalTables})</h6>
                <ul class="list-unstyled">
                    ${workflowData.selectedTables.map(table =>
        `<li><i class="fas fa-check text-success me-2"></i>${table}</li>`
    ).join('')}
                </ul>
            </div>
            <div class="col-md-6">
                <h6><i class="fas fa-link me-2 text-primary"></i>Relationships Defined (${totalRelationships})</h6>
                <ul class="list-unstyled">
                    ${workflowData.relationships.map(rel =>
        `<li>
                            <i class="fas fa-arrow-right text-primary me-2"></i>
                            ${rel.parentTable}.${rel.parentField} → ${rel.childTable}.${rel.childField}
                            <span class="badge bg-secondary ms-1">${rel.type}</span>
                        </li>`
    ).join('')}
                </ul>
            </div>
        </div>
        
        <div class="row mt-3">
            <div class="col-md-4">
                <div class="card bg-primary text-white">
                    <div class="card-body text-center">
                        <h3>${totalTables}</h3>
                        <small>Tables</small>
                    </div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card bg-success text-white">
                    <div class="card-body text-center">
                        <h3>${totalFields}</h3>
                        <small>Total Fields</small>
                    </div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card bg-warning text-white">
                    <div class="card-body text-center">
                        <h3>${totalRelationships}</h3>
                        <small>Relationships</small>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mt-3">
            <div class="col-md-12">
                <div class="alert alert-success">
                    <h6><i class="fas fa-check-circle me-2"></i>Ready to Generate Mapping!</h6>
                    <div class="row">
                        <div class="col-md-6">
                            <strong>Field Types:</strong> ${getFieldTypeSummary()}
                        </div>
                        <div class="col-md-6">
                            <strong>Relationship Types:</strong> ${relationshipTypes.join(', ')}
                        </div>
                    </div>
                    <hr class="my-2">
                    <small class="text-muted">
                        <i class="fas fa-lightbulb me-1"></i>
                        Your mapping will include ${totalFields} fields across ${totalTables} tables with ${totalRelationships} relationships.
                    </small>
                </div>
            </div>
        </div>
    `;
}

function calculateTotalFields() {
    let totalFields = 0;

    workflowData.selectedTables.forEach(tableName => {
        const tableFields = workflowData.tableStructures[tableName] || [];
        totalFields += tableFields.length;
    });

    return totalFields;
}

// 🆕 NEW FUNCTION: Get unique relationship types
function getRelationshipTypes() {
    const types = new Set();
    workflowData.relationships.forEach(rel => {
        types.add(rel.type);
    });
    return Array.from(types);
}

// 🆕 NEW FUNCTION: Get field type summary
function getFieldTypeSummary() {
    const typeCounts = {};

    workflowData.selectedTables.forEach(tableName => {
        const tableFields = workflowData.tableStructures[tableName] || [];
        tableFields.forEach(field => {
            const oracleType = getFieldType(field);
            const elasticType = oracleToElasticType(oracleType);
            typeCounts[elasticType] = (typeCounts[elasticType] || 0) + 1;
        });
    });

    return Object.entries(typeCounts)
        .map(([type, count]) => `${count} ${type}`)
        .join(', ');
}

// 🆕 NEW FUNCTION: Auto-populate configuration fields
function autoPopulateConfigFields() {
    const mappingNameInput = document.getElementById('workflowMappingName');
    const indexNameInput = document.getElementById('workflowIndexName');

    if (mappingNameInput && !mappingNameInput.value) {
        const suggestedName = `${workflowData.selectedTables.join('_')}_mapping`;
        mappingNameInput.value = suggestedName;
        mappingNameInput.placeholder = suggestedName;
    }

    if (indexNameInput && !indexNameInput.value) {
        const suggestedIndex = `${workflowData.selectedTables[0].toLowerCase()}_index`;
        indexNameInput.value = suggestedIndex;
        indexNameInput.placeholder = suggestedIndex;
    }
}

function checkAndFixTableStructures() {
    console.log("🔍 Checking table structures...");
    console.log("Selected tables:", workflowData.selectedTables);
    console.log("Table structures:", Object.keys(workflowData.tableStructures));

    // Check if we have structures for all selected tables
    const missingStructures = [];
    workflowData.selectedTables.forEach(table => {
        if (!workflowData.tableStructures[table] || workflowData.tableStructures[table].length === 0) {
            missingStructures.push(table);
        }
    });

    if (missingStructures.length > 0) {
        console.log(`❌ Missing structures for: ${missingStructures.join(', ')}`);
        showAlert(`Missing table structures for: ${missingStructures.join(', ')}. Loading now...`, 'warning');

        // Auto-load missing structures
        loadMissingTableStructures(missingStructures);
    } else {
        console.log("✅ All table structures are available");
        // Regenerate field mappings with correct structures
        generateFieldMappings();
    }
}


async function loadMissingTableStructures(missingTables) {
    if (!workflowData.selectedEnvironment) {
        showAlert('No Oracle environment selected. Please go back to Step 1.', 'error');
        return;
    }

    try {
        console.log(`🔄 Loading structures for missing tables: ${missingTables.join(', ')}`);

        // Show loading indicator
        const container = document.getElementById('fieldMappingContainer');
        if (container) {
            container.innerHTML = `
                <div class="text-center py-5">
                    <div class="spinner-border text-primary mb-3"></div>
                    <p>Loading missing table structures...</p>
                    <small class="text-muted">Tables: ${missingTables.join(', ')}</small>
                </div>
            `;
        }

        // Load structures for missing tables
        const promises = missingTables.map(tableName =>
            fetch(`/oracle/table-structure/${workflowData.selectedEnvironment}/${tableName}`)
                .then(response => response.json())
                .then(result => ({ tableName, result }))
        );

        const results = await Promise.all(promises);

        // Process results
        results.forEach(({ tableName, result }) => {
            if (result.success && result.columns) {
                workflowData.tableStructures[tableName] = result.columns;
                console.log(`✅ Loaded structure for ${tableName}: ${result.columns.length} columns`);
            } else {
                console.error(`❌ Failed to load structure for ${tableName}:`, result.error);
            }
        });

        // Check if all structures are now loaded
        const stillMissing = missingTables.filter(table =>
            !workflowData.tableStructures[table] || workflowData.tableStructures[table].length === 0
        );

        if (stillMissing.length === 0) {
            showAlert('✅ All table structures loaded successfully!', 'success');
            // Regenerate field mappings
            generateFieldMappings();
        } else {
            showAlert(`❌ Still missing structures for: ${stillMissing.join(', ')}`, 'error');
        }

    } catch (error) {
        console.error('Error loading missing table structures:', error);
        showAlert('Error loading table structures: ' + error.message, 'danger');
    }
}





function createAIFieldElement(originalField, embeddingType) {
    const fieldElement = document.createElement('div');
    fieldElement.className = 'ai-field-item';

    const embeddingFieldName = `${originalField.field_name}_${embeddingType}`;
    const embeddingConfig = getEmbeddingConfig(embeddingType);

    fieldElement.innerHTML = `
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <i class="fas fa-robot text-dark me-2"></i>
                        <strong class="text-dark">${embeddingFieldName}</strong>
                        <span class="embedding-type-badge ms-2">${embeddingConfig.displayName}</span>
                    </div>
                    <div class="text-muted">
                        <small>Dimensions: ${embeddingConfig.dimensions}</small>
                    </div>
                </div>
                <div class="mt-2">
                    <small class="text-muted">Generated from: ${originalField.field_name}</small>
                </div>
            `;

    return fieldElement;
}

function getEmbeddingConfig(embeddingType) {
    const configs = {
        'semantic_embedding': {
            displayName: 'Semantic Embedding',
            dimensions: 384,
            model: 'sentence-transformers/all-MiniLM-L6-v2'
        },
        'hybrid_embedding': {
            displayName: 'Hybrid Embedding',
            dimensions: 768,
            model: 'hybrid-search-v1'
        },
        'ai_embedding': {
            displayName: 'AI Embedding',
            dimensions: 1536,
            model: 'text-embedding-ada-002'
        }
    };
    return configs[embeddingType] || configs['semantic_embedding'];
}


function toggleSearchType(searchType) {
    const toggle = document.getElementById(`${searchType}SearchToggle`);
    aiSearchConfig[searchType] = toggle.checked;

    const advancedPanel = document.getElementById('advancedConfigPanel');
    const advancedSettings = document.getElementById('advancedSettings');
    const applyBtn = document.getElementById('applyConfigBtn');

    // Show advanced panel if any search type is enabled
    const hasAnyEnabled = Object.values(aiSearchConfig).some(enabled => enabled);
    if (hasAnyEnabled) {
        advancedPanel.style.display = 'block';
        applyBtn.disabled = false;
        applyBtn.classList.remove('btn-secondary');
        applyBtn.classList.add('btn-success');
    } else {
        advancedPanel.style.display = 'none';
        applyBtn.disabled = true;
        applyBtn.classList.remove('btn-success');
        applyBtn.classList.add('btn-secondary');
    }

    // Update advanced settings content
    updateAdvancedSettings();
    updateConfigurationStatus();
}

function updateAdvancedSettings() {
    const advancedSettings = document.getElementById('advancedSettings');
    let settingsHtml = '';

    if (aiSearchConfig.semantic) {
        settingsHtml += `
                    <div class="col-md-6">
                        <div class="card border-primary">
                            <div class="card-header bg-primary text-white">
                                <h6 class="mb-0"><i class="fas fa-vector-square me-2"></i>Semantic Search</h6>
                            </div>
                            <div class="card-body">
                                <div class="mb-2">
                                    <label class="form-label">Embedding Model</label>
                                    <select class="form-select form-select-sm" id="embeddingModel">
                                        <option value="sentence-transformers">sentence-transformers</option>
                                        <option value="openai">OpenAI ada-002</option>
                                        <option value="cohere">Cohere v3.0</option>
                                    </select>
                                </div>
                                <div class="mb-2">
                                    <label class="form-label">Vector Dimensions</label>
                                    <input type="number" class="form-control form-control-sm" id="vectorDimensions" value="384" min="128" max="1536">
                                </div>
                            </div>
                        </div>
                    </div>
                `;
    }

    if (aiSearchConfig.hybrid) {
        settingsHtml += `
                    <div class="col-md-6">
                        <div class="card border-success">
                            <div class="card-header bg-success text-white">
                                <h6 class="mb-0"><i class="fas fa-balance-scale me-2"></i>Hybrid Search</h6>
                            </div>
                            <div class="card-body">
                                <div class="mb-2">
                                    <label class="form-label">Lexical Weight: <span id="lexicalWeightValue">0.5</span></label>
                                    <input type="range" class="form-range" id="lexicalWeight" min="0" max="1" step="0.1" value="0.5" oninput="updateRangeValue('lexical')">
                                </div>
                                <div class="mb-2">
                                    <label class="form-label">Semantic Weight: <span id="semanticWeightValue">0.5</span></label>
                                    <input type="range" class="form-range" id="semanticWeight" min="0" max="1" step="0.1" value="0.5" oninput="updateRangeValue('semantic')">
                                </div>
                            </div>
                        </div>
                    </div>
                `;
    }

    if (aiSearchConfig.ai) {
        settingsHtml += `
                    <div class="col-md-6">
                        <div class="card border-warning">
                            <div class="card-header bg-warning text-dark">
                                <h6 class="mb-0"><i class="fas fa-brain me-2"></i>AI Search</h6>
                            </div>
                            <div class="card-body">
                                <div class="mb-2">
                                    <label class="form-label">AI Provider</label>
                                    <select class="form-select form-select-sm" id="aiProvider">
                                        <option value="openai">OpenAI GPT-4</option>
                                        <option value="anthropic">Anthropic Claude</option>
                                        <option value="google">Google Gemini</option>
                                    </select>
                                </div>
                                <div class="mb-2">
                                    <label class="form-label">Context Window</label>
                                    <input type="number" class="form-control form-control-sm" id="contextWindow" value="4096" min="1024" max="32768">
                                </div>
                            </div>
                        </div>
                    </div>
                `;
    }

    advancedSettings.innerHTML = settingsHtml;
}

function updateRangeValue(type) {
    const range = document.getElementById(`${type}Weight`);
    const valueSpan = document.getElementById(`${type}WeightValue`);
    if (range && valueSpan) {
        valueSpan.textContent = range.value;
    }
}

function updateConfigurationStatus() {
    const configStatus = document.getElementById('configStatus');
    const enabledFeatures = [];

    if (aiSearchConfig.semantic) enabledFeatures.push('Semantic Search');
    if (aiSearchConfig.hybrid) enabledFeatures.push('Hybrid Search');
    if (aiSearchConfig.ai) enabledFeatures.push('AI Search');

    let statusHtml = '';

    if (enabledFeatures.length === 0) {
        statusHtml = `
                    <div class="text-muted text-center">
                        <i class="fas fa-info-circle fa-2x mb-2"></i>
                        <p>No AI search features enabled</p>
                    </div>
                `;
    } else {
        statusHtml = `
                    <div class="text-center">
                        <div class="text-success mb-2">
                            <i class="fas fa-check-circle fa-2x"></i>
                        </div>
                        <h6 class="text-success">AI Search Features Enabled</h6>
                        <div class="mt-2">
                `;

        enabledFeatures.forEach(feature => {
            const iconMap = {
                'Semantic Search': 'fas fa-search text-primary',
                'Hybrid Search': 'fas fa-layer-group text-success',
                'AI Search': 'fas fa-robot text-warning'
            };
            statusHtml += `<span class="badge bg-light text-dark me-1"><i class="${iconMap[feature]} me-1"></i>${feature}</span>`;
        });

        statusHtml += `
                        </div>
                        <small class="text-muted mt-2 d-block">Click "Apply Configuration" to generate AI fields</small>
                    </div>
                `;
    }

    configStatus.innerHTML = statusHtml;
}
function applyAIConfiguration() {
    const aiSection = document.getElementById('aiSection');
    const aiFieldsContainer = document.getElementById('aiFieldsContainer');

    // Show AI section if any AI search type is enabled
    const hasAnyAISearch = aiSearchConfig.semantic || aiSearchConfig.hybrid || aiSearchConfig.ai;

    if (hasAnyAISearch) {
        aiSection.style.display = 'block';
        aiSection.classList.add('ai-search-enabled');

        // Clear existing AI fields
        aiFieldsContainer.innerHTML = '';

        // Generate AI fields for each enabled search type
        mappingFields.forEach(field => {
            if (field.field_type === 'text' || field.field_type === 'keyword') {
                if (aiSearchConfig.semantic) {
                    const aiField = createAIFieldElement(field, 'semantic_embedding');
                    aiFieldsContainer.appendChild(aiField);

                    // Add to mapping fields
                    const embeddingField = {
                        field_name: `${field.field_name}_semantic_embedding`,
                        oracle_type: 'AI_GENERATED',
                        elastic_type: 'dense_vector',
                        field_type: 'dense_vector',
                        section: 'ai',
                        properties: {
                            "type": "dense_vector",
                            "dims": getEmbeddingConfig('semantic_embedding').dimensions
                        },
                        ai_config: {
                            source_field: field.field_name,
                            embedding_type: 'semantic',
                            model: getEmbeddingConfig('semantic_embedding').model
                        }
                    };

                    // Check if field doesn't already exist
                    if (!mappingFields.find(f => f.field_name === embeddingField.field_name)) {
                        mappingFields.push(embeddingField);
                    }
                }

                if (aiSearchConfig.hybrid) {
                    const aiField = createAIFieldElement(field, 'hybrid_embedding');
                    aiFieldsContainer.appendChild(aiField);

                    const embeddingField = {
                        field_name: `${field.field_name}_hybrid_embedding`,
                        oracle_type: 'AI_GENERATED',
                        elastic_type: 'dense_vector',
                        field_type: 'dense_vector',
                        section: 'ai',
                        properties: {
                            "type": "dense_vector",
                            "dims": getEmbeddingConfig('hybrid_embedding').dimensions
                        },
                        ai_config: {
                            source_field: field.field_name,
                            embedding_type: 'hybrid',
                            model: getEmbeddingConfig('hybrid_embedding').model,
                            lexical_weight: document.getElementById('lexicalWeight')?.value || 0.5,
                            semantic_weight: document.getElementById('semanticWeight')?.value || 0.5
                        }
                    };

                    if (!mappingFields.find(f => f.field_name === embeddingField.field_name)) {
                        mappingFields.push(embeddingField);
                    }
                }

                if (aiSearchConfig.ai) {
                    const aiField = createAIFieldElement(field, 'ai_embedding');
                    aiFieldsContainer.appendChild(aiField);

                    const embeddingField = {
                        field_name: `${field.field_name}_ai_embedding`,
                        oracle_type: 'AI_GENERATED',
                        elastic_type: 'dense_vector',
                        field_type: 'dense_vector',
                        section: 'ai',
                        properties: {
                            "type": "dense_vector",
                            "dims": getEmbeddingConfig('ai_embedding').dimensions
                        },
                        ai_config: {
                            source_field: field.field_name,
                            embedding_type: 'ai',
                            model: getEmbeddingConfig('ai_embedding').model,
                            provider: document.getElementById('aiProvider')?.value || 'openai',
                            context_window: document.getElementById('contextWindow')?.value || 4096
                        }
                    };

                    if (!mappingFields.find(f => f.field_name === embeddingField.field_name)) {
                        mappingFields.push(embeddingField);
                    }
                }
            }
        });

        if (aiFieldsContainer.children.length === 0) {
            aiFieldsContainer.innerHTML = `
                        <div class="text-center text-muted">
                            <i class="fas fa-exclamation-triangle fa-2x mb-2"></i>
                            <p>No text fields available for AI enhancement</p>
                            <small>Add text fields to the Parent-Child Relations section first</small>
                        </div>
                    `;
        }
        renderVectorEmbeddingFields();
    } else {
        aiSection.classList.remove('ai-search-enabled');

        // Remove AI-generated fields from mappingFields
        mappingFields = mappingFields.filter(field => field.section !== 'ai');
        aiFieldsContainer.innerHTML = '';
        renderVectorEmbeddingFields();
        if (mappingFields.some(f => f.section === 'vector')) {
            aiSection.style.display = 'block';
        } else {
            aiSection.style.display = 'none';
        }
    }

    // Update the mapping display and preview
    updateMappingBuilderDisplay();
    updateElasticsearchMappingPreview();

    showAlert('AI Configuration applied successfully!', 'success');
}

async function loadElasticsearchEnvironmentsForUpdate() {
    try {
        const response = await fetch('/environments');
        const environments = await response.json();
        const select = document.getElementById('updateEnvSelect');
        select.innerHTML = '<option value="">Select environment...</option>';
        (environments.elasticsearch || []).forEach(env => {
            const opt = document.createElement('option');
            opt.value = env.id;
            opt.textContent = env.name;
            select.appendChild(opt);
        });
    } catch (err) {
        console.error('Error loading Elasticsearch environments', err);
        showAlert('Error loading Elasticsearch environments', 'danger');
    }
}

async function loadIndicesForUpdate(envId, selected) {
    const indexSelect = document.getElementById('updateIndexSelect');
    indexSelect.innerHTML = '<option value="">Select index...</option>';
    // Clear field selects until an index is chosen
    ['updateRootFields', 'updateParentChildFields', 'updateNestedFields', 'updateAIFields'].forEach(id => {
        const sel = document.getElementById(id);
        if (sel) sel.innerHTML = '';
    });
    if (!envId) return;
    try {
        const response = await fetch(`/indices/${envId}`);
        const indices = await response.json();
        indices.forEach(idx => {
            const opt = document.createElement('option');
            opt.value = idx.index;
            opt.textContent = idx.index;
            if (selected && selected === idx.index) opt.selected = true;
            indexSelect.appendChild(opt);
        });
        // If an index is preselected, load its fields
        const chosen = selected || indexSelect.value;
        if (chosen) {
            loadUpdateMappingFields(envId, chosen);
        }
    } catch (err) {
        console.error('Error loading indices', err);
        showAlert('Error loading indices', 'danger');
    }
}

async function loadUpdateMappingFields(envId, indexName) {
    const rootSelect = document.getElementById('updateRootFields');
    const parentSelect = document.getElementById('updateParentChildFields');
    const nestedSelect = document.getElementById('updateNestedFields');
    const aiSelect = document.getElementById('updateAIFields');
    const relationGroup = document.getElementById('updateRelationsGroup');
    const relationSelect = document.getElementById('updateRelations');

    [rootSelect, parentSelect, nestedSelect, aiSelect].forEach(sel => sel.innerHTML = '');
    relationSelect.innerHTML = '';
    relationGroup.style.display = 'none';
    if (!envId || !indexName) return;

    let savedData = {};
    try {
        const savedRes = await fetch(`/mapping-update/${envId}/${indexName}`);
        if (savedRes.ok) {
            savedData = await savedRes.json();
        }
    } catch (e) {
        console.warn('No existing mapping update', e);
    }

    const preRoot = savedData.root_fields || [];
    const preParent = savedData.parent_child_fields || [];
    const preNested = savedData.nested_fields || [];
    const preAI = savedData.ai_fields || [];
    const selectedRelation = savedData.parent_child_relation || '';

    try {
        const res = await fetch(`/mapping/${envId}/${indexName}`);
        const data = await res.json();

        // Support both `{index: {mappings:{properties}}}` and `{mappings:{properties}}` formats
        let props = null;
        if (data.mapping) {
            if (data.mapping.mappings?.properties) {
                props = data.mapping.mappings.properties;
            } else {
                const mapping = data.mapping[indexName] || Object.values(data.mapping)[0];
                props = mapping?.mappings?.properties || null;
            }
        }

        if (!props) {
            console.warn('No properties found in mapping response');
            return;
        }

        const fields = extractFieldsFromMapping({ properties: props });
        const names = fields.map(f => f.name);
        const rootNames = names.filter(n => !n.includes('.'));
        const joinFields = fields.filter(f => f.originalConfig.type === 'join');

        const populate = (select, list, selected=[]) => {
            list.forEach(n => {
                const opt = document.createElement('option');
                opt.value = n;
                opt.textContent = n;
                if (selected.includes(n)) opt.selected = true;
                select.appendChild(opt);
            });
        };

        populate(rootSelect, rootNames, preRoot);
        populate(parentSelect, names, preParent);
        populate(nestedSelect, names, preNested);
        populate(aiSelect, names, preAI);

        if (joinFields.length > 0) {
            relationGroup.style.display = 'block';
            const relations = joinFields[0].originalConfig.relations || {};
            Object.entries(relations).forEach(([parent, child]) => {
                const opt = document.createElement('option');
                const value = `${parent}:${child}`;
                opt.value = value;
                opt.textContent = `${parent} → ${child}`;
                if (value === selectedRelation) opt.selected = true;
                relationSelect.appendChild(opt);
            });
        }
    } catch (err) {
        console.error('Error loading mapping fields', err);
        showAlert('Error loading mapping fields', 'danger');
    }
}

function showUpdateMappingModal() {
    const envSelect = document.getElementById('updateEnvSelect');
    const indexSelect = document.getElementById('updateIndexSelect');

    loadElasticsearchEnvironmentsForUpdate().then(() => {
        const currentEnv = document.getElementById('mappingEnvironment');
        const currentIndex = document.getElementById('mappingIndex');
        if (currentEnv && currentEnv.value.startsWith('elasticsearch-')) {
            const envId = currentEnv.value.split('-')[1];
            envSelect.value = envId;
            loadIndicesForUpdate(envId, currentIndex ? currentIndex.value : undefined);
        }
    });

    envSelect.onchange = () => loadIndicesForUpdate(envSelect.value);
    indexSelect.onchange = () => loadUpdateMappingFields(envSelect.value, indexSelect.value);

    new bootstrap.Modal(document.getElementById('updateMappingModal')).show();
}
// Expose for inline onclick handlers
window.showUpdateMappingModal = showUpdateMappingModal;

async function saveMappingUpdate() {
    const envId = document.getElementById('updateEnvSelect').value;
    const index = document.getElementById('updateIndexSelect').value;
    const rootFields = Array.from(document.getElementById('updateRootFields').selectedOptions).map(o => o.value);
    const parentFields = Array.from(document.getElementById('updateParentChildFields').selectedOptions).map(o => o.value);
    const nestedFields = Array.from(document.getElementById('updateNestedFields').selectedOptions).map(o => o.value);
    const aiFields = Array.from(document.getElementById('updateAIFields').selectedOptions).map(o => o.value);
    const relation = document.getElementById('updateRelations').value;

    if (!envId || !index) {
        showAlert('Please select an environment and index', 'warning');
        return;
    }

    try {
        const response = await fetch('/save-mapping-update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                env_id: parseInt(envId),
                index_name: index,
                root_fields: rootFields,
                parent_child_fields: parentFields,
                parent_child_relation: relation || null,
                nested_fields: nestedFields,
                ai_fields: aiFields
            })
        });
        const data = await response.json();
        if (data.success) {
            const rootSet = new Set(rootFields);
            const parentSet = new Set(parentFields);
            const nestedSet = new Set(nestedFields);
            const aiSet = new Set(aiFields);

            mappingFields.forEach(f => {
                if (parentSet.has(f.field_name)) f.section = 'parent-child';
                else if (nestedSet.has(f.field_name)) f.section = 'nested';
                else if (aiSet.has(f.field_name)) f.section = 'ai';
                else if (rootSet.has(f.field_name)) f.section = 'root';
            });

            updateMappingBuilderDisplay();
            showAlert('Mapping update saved successfully!', 'success');
            bootstrap.Modal.getInstance(document.getElementById('updateMappingModal')).hide();
        } else {
            showAlert('Failed to save mapping update', 'error');
        }
    } catch (err) {
        showAlert('Error saving mapping update', 'error');
    }
}
// Expose save handler globally
window.saveMappingUpdate = saveMappingUpdate;

