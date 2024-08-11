
struct QueryPipeline<
  _StorageSelector: StorageSelector,
  Output
>: PipelineType, Sendable {

  typealias Input = Changes<_StorageSelector.Source>
  typealias EntityStorage = _StorageSelector.Storage

  private let storageSelector: _StorageSelector
  private let query: @Sendable (EntityStorage) -> Output

  init(
    storageSelector: consuming _StorageSelector,
    query: @escaping @Sendable (EntityStorage) -> Output
  ) {
    self.storageSelector = storageSelector
    self.query = query
  }

  func makeStorage() -> Void {
    ()
  }

  func yield(_ input: consuming Input, storage: Void) -> Output {

    let storage = storageSelector.select(source: input.primitive)
    let output = query(storage)

    return output

  }

  func yieldContinuously(_ input: Input, storage: Void) -> Verge.ContinuousResult<Output> {

    guard let previous = input.previous else {
      return .new(yield(input, storage: storage))
    }

    // check if the storage has been updated
    if NormalizedStorageComparisons<EntityStorage>.StorageComparison()(storageSelector.select(source: input.primitive), storageSelector.select(source: previous.primitive)) {
      return .noUpdates
    }

    return .new(yield(input, storage: storage))

  }

}
