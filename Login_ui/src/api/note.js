// src/api/note.js
import service from './request'; // 确保路径正确

// =========================================================
//                  辅助函数 (Adapters)
// =========================================================

/**
 * 格式化笔记元数据为 JSON 字符串，用于 multipart 请求
 * @param {object} meta - 笔记的元数据对象 (NoteMeta 或 NoteUpdateMeta)
 * @returns {string}
 */
function formatMeta(meta) {
    const processedMeta = {};
    for (const key in meta) {
        if (Object.prototype.hasOwnProperty.call(meta, key)) {
            // 将所有数字（如 ID）转换为字符串，与 Long 类型匹配
            processedMeta[key] = (typeof meta[key] === 'number') ? String(meta[key]) : meta[key];
        }
    }
    return JSON.stringify(processedMeta);
}

/**
 * 将数据转换为 FormData 格式，用于文件上传（Multipart）
 * @param {string} metaJson - 笔记元数据 JSON 字符串 (后端 @RequestPart("meta"))
 * @param {File | null} file - 文件对象 (后端 @RequestPart("file"))
 * @returns {FormData}
 */
function createFormData(metaJson, file) {
    const formData = new FormData();
    formData.append('meta', metaJson);

    if (file) {
        formData.append('file', file);
    }
    return formData;
}

// =========================================================
//                        笔记 CRUD API
// =========================================================

/**
 * [对应后端: POST /noting/notes/by-notebook]
 * 获取指定笔记本下的笔记列表 (返回 List<NoteVO>)
 */
export const fetchNotesByNotebook = (notebookId) => {
    const requestBody = {
        notebookID: notebookId,
    };
    // 【API调用点 C】
    return service.post('/noting/notes/by-notebook', requestBody)
        .then(res => res.data.data); // 提取 StandardResponse 中的 data 字段
};

/**
 * [对应后端: POST /noting/notes/create]
 * 创建笔记（富文本或文件）(返回 NoteVO)
 */
export const createNote = (meta, file) => {
    const metaJson = formatMeta(meta);
    const formData = createFormData(metaJson, file);

    // 【API调用点 G/H】
    return service.post('/noting/notes/create', formData, {
        headers: {
            'Content-Type': 'multipart/form-data'
        }
    }).then(res => res.data.data);
};

/**
 * [对应后端: POST /noting/notes/files]
 * 上传笔记（富文本或文件）(返回 NoteVO)
 */
export const uploadNote = (meta, file) => {
    const metaJson = formatMeta(meta);
    const formData = createFormData(metaJson, file);

    // 【API调用点 G/H】
    return service.post('/noting/notes/files', formData, {
        headers: {
            'Content-Type': 'multipart/form-data'
        }
    }).then(res => res.data.data);
};

/**
 * [对应后端: PUT /noting/notes/update]
 * 更新笔记（内容、标题、文件等）(返回 NoteVO)
 */
export const updateNote = (meta, file = null) => {
    const metaJson = formatMeta(meta);
    const formData = createFormData(metaJson, file);

    // 【API调用点 B/D/I】
    return service.put('/noting/notes/update', formData, {
        headers: {
            'Content-Type': 'multipart/form-data'
        }
    }).then(res => res.data.data);
};

/**
 * [对应后端: PUT /noting/notes/rename]
 * 重命名笔记（内容、标题、文件等）(返回 NoteVO)
 */
export const renameNote = (id, newName) => {

    const requestBody = {
        id: id,
        newName: newName
    };
    // 【API调用点 C】
    return service.post('/noting/notes/rename', requestBody)
        .then(res => res.data.data);
};

/**
 * [对应后端: DELETE /noting/notes]
 * 删除笔记 (返回 Void)
 */
export const deleteNote = (noteId) => {
    const requestBody = {
        noteId: noteId
    };
    // 【API调用点 E】
    return service.delete('/noting/notes', { data: requestBody })
        .then(res => res.data.data);
};

/**
 * [对应后端: POST /noting/notes/move]
 * 移动笔记到另一个笔记本 (返回 NoteVO)
 */
export const moveNote = (noteId, targetNotebookId) => {
    const requestBody = {
        noteId: noteId,
        targetNotebookId: targetNotebookId
    };
    return service.post('/noting/notes/move', requestBody)
        .then(res => res.data.data);
};

/**
 * [对应后端: POST /noting/notes/publish]
 * 发布笔记（带文件和元数据）(返回 NoteVO)
 */
export const publishNote = (meta, file) => {
    const metaJson = formatMeta(meta);
    const formData = createFormData(metaJson, file);

    return service.post('/noting/notes/publish', formData, {
        headers: {
            'Content-Type': 'multipart/form-data'
        }
    }).then(res => res.data.data);
};

// =========================================================
//                       文件/图片操作 API
// =========================================================

/**
 * [对应后端: POST /noting/notes/image]
 * 上传图片并获取 URL (返回 String)
 */
export const uploadImage = (imageFile) => {
    const formData = new FormData();
    formData.append('file', imageFile);

    // 【API调用点 J】
    return service.post('/noting/notes/image', formData, {
        headers: {
            'Content-Type': 'multipart/form-data'
        }
    }).then(res => res.data.data);
};

/**
 * [对应后端: POST /noting/notes/files/url]
 * 获取文件访问 URL (返回 String)
 */
export const getFileUrl = (filename) => {
    const requestBody = {
        filename: filename
    };
    return service.post('/noting/notes/files/url', requestBody)
        .then(res => res.data.data);
};

/**
 * [对应后端: POST /noting/notes/files/id_url]
 * 通过笔记ID获取笔记信息 (返回 NoteShowVO 对象，包含 id, title, url, fileType 等)
 * @param {number} noteId - 笔记ID
 * @returns {Promise<{id: number, title: string, url: string, fileType: string, notebookId: number, createdAt: string, updatedAt: string}>}
 */
export const getFileUrlByNoteId = (noteId) => {
    // 后端使用 @RequestParam，所以使用查询参数
    return service.post(`/noting/notes/files/id_url`, null, {
        params: { noteId: noteId }
    })
        .then(res => res.data.data);
};

// =========================================================
//                        笔记统计 API
// =========================================================

/**
 * [对应后端: GET /noting/note-stats/{noteId}]
 * 获取笔记统计信息 (返回 NoteStatsVO)
 * @param {number} noteId - 笔记ID
 */
export const getNoteStats = (noteId) => {
    return service.get(`/noting/note-stats/${noteId}`)
        .then(res => res.data.data);
};

/**
 * [对应后端: POST /noting/note-stats/change]
 * 变更笔记统计字段（views/likes/favorites/comments）
 * @param {number} noteId - 笔记ID
 * @param {number} userId - 用户ID
 * @param {string} field - 字段名：views | likes | favorites | comments
 * @param {number} delta - 变化值，默认+1，可传-1做撤销
 */
export const changeNoteStat = (noteId, userId, field, delta = 1) => {
    return service.post('/noting/note-stats/change', null, {
        params: { noteId, userId, field, delta }
    }).then(res => res.data.data);
};

// =========================================================
//                        搜索 API
// =========================================================

/**
 * [对应后端: POST /api/v1/search/notes]
 * 搜索笔记 (返回 List<NoteSearchVO>)
 * @param {string} keyword - 搜索关键词
 * @param {number} userId - 用户ID
 */
export const searchNotes = (keyword, userId) => {
    const requestBody = {
        keyword: keyword,
        userId: userId
    };
    return service.post('/search/notes', requestBody)
        .then(res => res.data.data);
};

/**
 * [对应后端: GET /hot/notes]
 * 获取热门笔记榜单 (返回 List<NoteSearchVO>)
 */
export const getHotNotes = () => {
    return service.get('/hot/notes')
        .then(res => res.data.data);
};

/**
 * [对应后端: GET /recommend/notes]
 * 获取推荐笔记列表 (返回 List<NoteSearchVO>)
 * @param {number} userId - 用户ID
 * @param {number} topN - 返回条数，默认 10
 */
export const getRecommendedNotes = (userId, topN = 10) => {
    return service.get('/recommend/notes', {
        params: { userId, topN }
    }).then(res => res.data.data);
};