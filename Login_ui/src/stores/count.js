import { defineStore } from 'pinia'

// 定义一个名为 counter 的 store
export const useCounterStore = defineStore('counter', {
    state: () => ({
        count: 0  // 共享状态
    }),
    actions: {
        increment() {
            this.count++  // 修改状态
        },
        decrement() {
            this.count--
        }
    }
})
