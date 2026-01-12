// src/api/follow.js
import service from './request';

// =========================================================
//                        关注关系 API
// =========================================================

/**
 * [对应后端: GET /api/v1/followRelation/followings]
 * 获取关注列表 (返回 GetFollowingsVO)
 * @param {number} userId - 用户ID
 * @returns {Promise<{user_id: number, followings: Array<{userId: number, followTime: string}>}>}
 */
export const getFollowings = (userId) => {
    // 确保 userId 是数字类型
    const id = Number(userId);
    if (isNaN(id) || id <= 0) {
        return Promise.reject(new Error('无效的用户ID'));
    }
    return service.get('/followRelation/followings', {
        params: { userId: id }
    }).then(res => res.data.data);
};

/**
 * [对应后端: GET /api/v1/followRelation/followers]
 * 获取粉丝列表 (返回 GetFollowersVO)
 * @param {number} userId - 用户ID
 * @returns {Promise<{user_id: number, followers: Array<{userId: number, followTime: string}>}>}
 */
export const getFollowers = (userId) => {
    // 确保 userId 是数字类型
    const id = Number(userId);
    if (isNaN(id) || id <= 0) {
        return Promise.reject(new Error('无效的用户ID'));
    }
    return service.get('/followRelation/followers', {
        params: { userId: id }
    }).then(res => res.data.data);
};

/**
 * [对应后端: POST /api/v1/followRelation/follow]
 * 关注用户
 * @param {number} userId - 当前用户ID
 * @param {number} targetUserId - 目标用户ID
 * @returns {Promise<boolean>}
 */
export const followUser = (userId, targetUserId) => {
    // 确保参数是数字类型
    const uid = Number(userId);
    const tid = Number(targetUserId);
    if (isNaN(uid) || uid <= 0 || isNaN(tid) || tid <= 0) {
        return Promise.reject(new Error('无效的用户ID'));
    }
    return service.post('/followRelation/follow', null, {
        params: { userId: uid, targetUserId: tid }
    }).then(res => {
        // 后端返回 StandardResponse<Boolean>
        // 如果 message 不为空，说明有错误信息
        if (res.data.message && res.data.message !== 'success') {
            throw new Error(res.data.message);
        }
        return res.data.data;
    });
};

/**
 * [对应后端: POST /api/v1/followRelation/unfollow]
 * 取消关注用户
 * @param {number} userId - 当前用户ID
 * @param {number} targetUserId - 目标用户ID
 * @returns {Promise<boolean>}
 */
export const unfollowUser = (userId, targetUserId) => {
    // 确保参数是数字类型
    const uid = Number(userId);
    const tid = Number(targetUserId);
    if (isNaN(uid) || uid <= 0 || isNaN(tid) || tid <= 0) {
        return Promise.reject(new Error('无效的用户ID'));
    }
    return service.post('/followRelation/unfollow', null, {
        params: { userId: uid, targetUserId: tid }
    }).then(res => {
        // 后端返回 StandardResponse<Boolean>
        if (res.data.message && res.data.message !== 'success') {
            throw new Error(res.data.message);
        }
        return res.data.data;
    });
};

/**
 * [对应后端: GET /api/v1/followRelation/isFollowing]
 * 检查是否已关注
 * @param {number} userId - 当前用户ID
 * @param {number} targetUserId - 目标用户ID
 * @returns {Promise<boolean>}
 */
export const isFollowing = (userId, targetUserId) => {
    // 确保参数是数字类型
    const uid = Number(userId);
    const tid = Number(targetUserId);
    if (isNaN(uid) || uid <= 0 || isNaN(tid) || tid <= 0) {
        return Promise.reject(new Error('无效的用户ID'));
    }
    return service.get('/followRelation/isFollowing', {
        params: { userId: uid, targetUserId: tid }
    }).then(res => res.data.data);
};

/**
 * [对应后端: GET /api/v1/followRelation/isMutualFollow]
 * 检查是否互相关注
 * @param {number} userId - 当前用户ID
 * @param {number} targetUserId - 目标用户ID
 * @returns {Promise<boolean>}
 */
export const isMutualFollow = (userId, targetUserId) => {
    // 确保参数是数字类型
    const uid = Number(userId);
    const tid = Number(targetUserId);
    if (isNaN(uid) || uid <= 0 || isNaN(tid) || tid <= 0) {
        return Promise.reject(new Error('无效的用户ID'));
    }
    return service.get('/followRelation/isMutualFollow', {
        params: { userId: uid, targetUserId: tid }
    }).then(res => res.data.data);
};

/**
 * [对应后端: GET /api/v1/auth/user/by-username]
 * 根据用户名获取用户信息
 * @param {string} username - 用户名
 * @returns {Promise<{id: number, username: string, email: string, studentNumber: string, avatarUrl: string}>}
 */
export const getUserByUsername = (username) => {
    if (!username || typeof username !== 'string' || username.trim() === '') {
        return Promise.reject(new Error('用户名不能为空'));
    }
    return service.get('/auth/user/by-username', {
        params: { username: username.trim() }
    }).then(res => res.data);
};