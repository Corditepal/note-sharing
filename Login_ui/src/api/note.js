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