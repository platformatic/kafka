declare namespace NodeJS {
  export interface Process {
    _rawDebug: (...args: any[]) => void
  }
}
