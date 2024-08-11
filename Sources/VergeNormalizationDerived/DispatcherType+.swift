import Foundation

@_spi(NormalizedStorage) import Verge
@_spi(Internal) import Verge
import Verge

public enum NormalizedStorageError: Swift.Error {
  case notFoundEntityToMakeDerived
}

extension StoreType {

  public func normalizedStorage<Selector: StorageSelector>(_ selector: Selector) -> NormalizedStoragePath<Self, Selector> {
    .init(store: self, storageSelector: selector)
  }

}

/**
 The entrypoint to make Derived object from the storage
 */
public struct NormalizedStoragePath<
  Store: DerivedMaking & AnyObject,
  _StorageSelector: StorageSelector
>: ~Copyable where Store.State == _StorageSelector.Source {

  public typealias Storage = _StorageSelector.Storage
  unowned let store: Store
  let storageSelector: _StorageSelector

  public init(
    store: Store,
    storageSelector: _StorageSelector
  ) {

    self.store = store
    self.storageSelector = storageSelector
  }

  public func table<Selector: TableSelector>(
    _ selector: Selector
  ) -> NormalizedStorageTablePath<Store, _StorageSelector, Selector> where Selector.Storage == _StorageSelector.Storage {
    return .init(
      store: store,
      storageSelector: storageSelector,
      tableSelector: selector
    )
  }

  /**
   Make a new Derived of a composed object from the storage.
   This is an effective way to resolving relationship entities into a single object. it's like SQLite's view.

   ```
   store.normalizedStorage(.keyPath(\.db)).derived {
     MyComposed(
       book: $0.book.find(...)
       author: $0.author.find(...)
     )
   }
   ```

   This Derived makes a new composed object if the storage has updated.
   There is not filters for entity tables so that Derived possibly makes a new object if not related entity has updated.
   */
  public func derived<Composed: Equatable>(query: @escaping @Sendable (Self.Storage) -> Composed) -> Derived<Composed> {
    return store.derived(QueryPipeline(storageSelector: storageSelector, query: query), queue: .passthrough)
  }
}

/**
 The entrypoint to make Derived object from the specific table.
 */
public struct NormalizedStorageTablePath<
  Store: DerivedMaking & AnyObject,
  _StorageSelector: StorageSelector,
  _TableSelector: TableSelector
>: ~Copyable where _StorageSelector.Storage == _TableSelector.Storage, Store.State == _StorageSelector.Source {

  public typealias Entity = _TableSelector.Table.Entity

  unowned let store: Store
  let storageSelector: _StorageSelector
  let tableSelector: _TableSelector

  public func derived(
    from entityID: consuming Entity.EntityID
  ) -> Derived<EntityWrapper<Entity>> {

    return store.derivedEntity(
      selector: .init(storage: storageSelector, table: tableSelector),
      entityID: entityID
    )

  }

  public func derived(
    entityIDs: some Sequence<Entity.EntityID>
  ) -> DerivedResult<Entity, Derived<EntityWrapper<Entity>>> {

    var result = DerivedResult<Entity, Derived<EntityWrapper<Entity>>>()

    for id in entityIDs {
      result.append(
        derived: store.derivedEntity(
          selector: .init(storage: storageSelector, table: tableSelector),
          entityID: id
        ),
        id: id
      )
    }

    return result

  }

  public func derived(
    insertionResults: some Sequence<InsertionResult<Entity>>
  ) -> DerivedResult<Entity, Derived<EntityWrapper<Entity>>> {

    derived(entityIDs: insertionResults.map { $0.entityID })
  }

  // MARK: - NonNull

  /// Returns a derived object that provides a concrete entity according to the updating source state
  /// It uses the last value if the entity has been removed source.
  /// You can get a flag that indicates whether the entity is live or removed which from `NonNullEntityWrapper<T>`
  ///
  /// If you call this method in many time, it's not so big issue.
  /// Because, the backing derived-object to construct itself would be cached.
  /// A pointer of the result derived object will be different from each other, but the backing source will be shared.
  ///
  public func derivedNonNull(
    entity: consuming Entity
  ) -> Derived<NonNullEntityWrapper<Entity>> {

    return store.derivedEntityNonNull(
      selector: .init(storage: storageSelector, table: tableSelector),
      entity: entity
    )

  }

  /// Returns a derived object that provides a concrete entity according to the updating source state
  /// It uses the last value if the entity has been removed source.
  /// You can get a flag that indicates whether the entity is live or removed which from `NonNullEntityWrapper<T>`
  ///
  /// If you call this method in many time, it's not so big issue.
  /// Because, the backing derived-object to construct itself would be cached.
  /// A pointer of the result derived object will be different from each other, but the backing source will be shared.
  ///
  public func derivedNonNull(
    entityID: consuming Entity.EntityID
  ) throws -> Derived<NonNullEntityWrapper<Entity>> {

    let _initialValue = storageSelector
      .appending(tableSelector)
      .table(source: store.state.primitive)
      .find(by: entityID)

    guard let initalValue = _initialValue else {
      throw NormalizedStorageError.notFoundEntityToMakeDerived
    }

    return store.derivedEntityNonNull(
      selector: .init(storage: storageSelector, table: tableSelector),
      entity: initalValue
    )

  }

  public func derivedNonNull(
    entities: consuming some Sequence<Entity>
  ) -> NonNullDerivedResult<Entity> {

    var result = NonNullDerivedResult<Entity>()

    for entity in entities {
      result.append(
        derived: self.derivedNonNull(entity: entity),
        id: entity.entityID
      )
    }

    return result

  }

  /// Returns a derived object that provides a concrete entity according to the updating source state
  /// It uses the last value if the entity has been removed source.
  /// You can get a flag that indicates whether the entity is live or removed which from `NonNullEntityWrapper<T>`
  ///
  /// If you call this method in many time, it's not so big issue.
  /// Because, the backing derived-object to construct itself would be cached.
  /// A pointer of the result derived object will be different from each other, but the backing source will be shared.
  ///
  public func derivedNonNull(
    entityIDs: consuming some Sequence<Entity.EntityID>
  ) -> NonNullDerivedResult<Entity> {

    var result = NonNullDerivedResult<Entity>()

    for id in entityIDs {
      do {
        result.append(
          derived: try self.derivedNonNull(entityID: id),
          id: id
        )
      } catch {
        // FIXME:
      }
    }

    return result
  }

  /// Returns a derived object that provides a concrete entity according to the updating source state
  /// It uses the last value if the entity has been removed source.
  /// You can get a flag that indicates whether the entity is live or removed which from `NonNullEntityWrapper<T>`
  ///
  /// If you call this method in many time, it's not so big issue.
  /// Because, the backing derived-object to construct itself would be cached.
  /// A pointer of the result derived object will be different from each other, but the backing source will be shared.
  ///
  public func derivedNonNull(
    entityIDs: consuming Set<Entity.EntityID>
  ) -> NonNullDerivedResult<Entity> {

    var result = NonNullDerivedResult<Entity>()

    for id in entityIDs {
      do {
        result.append(
          derived: try self.derivedNonNull(entityID: id),
          id: id
        )
      } catch {
        // FIXME:
      }
    }

    return result
  }

  /// Returns a derived object that provides a concrete entity according to the updating source state
  /// It uses the last value if the entity has been removed source.
  /// You can get a flag that indicates whether the entity is live or removed which from `NonNullEntityWrapper<T>`
  ///
  /// If you call this method in many time, it's not so big issue.
  /// Because, the backing derived-object to construct itself would be cached.
  /// A pointer of the result derived object will be different from each other, but the backing source will be shared.
  ///
  public func derivedNonNull(
    insertionResult: InsertionResult<Entity>
  ) -> Entity.NonNullDerived {
    derivedNonNull(entity: insertionResult.entity)
  }

  /// Returns a derived object that provides a concrete entity according to the updating source state
  /// It uses the last value if the entity has been removed source.
  /// You can get a flag that indicates whether the entity is live or removed which from `NonNullEntityWrapper<T>`
  ///
  /// If you call this method in many time, it's not so big issue.
  /// Because, the backing derived-object to construct itself would be cached.
  /// A pointer of the result derived object will be different from each other, but the backing source will be shared.
  ///
  @inline(__always)
  public func derivedNonNull(
    insertionResults: some Sequence<InsertionResult<Entity>>
  ) -> NonNullDerivedResult<Entity> {
    derivedNonNull(entities: insertionResults.map { $0.entity })
  }
}

extension DerivedMaking {

  fileprivate func derivedEntity<
    _StorageSelector: StorageSelector,
    _TableSelector: TableSelector
  >(
    selector: AbsoluteTableSelector<_StorageSelector, _TableSelector>,
    entityID: consuming _TableSelector.Table.Entity.EntityID
  ) -> Derived<EntityWrapper<_TableSelector.Table.Entity>>
  where
    _StorageSelector.Storage == _TableSelector.Storage,
    _StorageSelector.Source == Self.State
  {

    return derived(
      SingleEntityPipeline(
        targetIdentifier: entityID,
        selector: selector
      ),
      queue: .passthrough
    )

  }

  fileprivate func derivedEntityNonNull<
    _StorageSelector: StorageSelector,
    _TableSelector: TableSelector
  >(
    selector: AbsoluteTableSelector<_StorageSelector, _TableSelector>,
    entity: consuming _TableSelector.Table.Entity
  ) -> Derived<NonNullEntityWrapper<_TableSelector.Table.Entity>>
  where
  _StorageSelector.Storage == _TableSelector.Storage,
  _StorageSelector.Source == Self.State
  {

    return derived(
      NonNullSingleEntityPipeline(
        initialEntity: entity,
        selector: selector
      ),
      queue: .passthrough
    )

  }
}

private struct SingleEntityPipeline<
  _StorageSelector: StorageSelector,
  _TableSelector: TableSelector
>: PipelineType
where _StorageSelector.Storage == _TableSelector.Storage {

  typealias Entity = _TableSelector.Table.Entity
  typealias Input = Changes<_StorageSelector.Source>
  typealias EntityStorage = _StorageSelector.Storage
  typealias Output = EntityWrapper<Entity>

  private let selector: AbsoluteTableSelector<_StorageSelector, _TableSelector>
  private let entityID: Entity.EntityID

  init(
    targetIdentifier: Entity.EntityID,
    selector: consuming AbsoluteTableSelector<_StorageSelector, _TableSelector>
  ) {
    self.entityID = targetIdentifier
    self.selector = selector
  }

  func yield(_ input: consuming Input, storage: Void) -> Output {

    let result = selector.table(source: input.primitive)
      .find(by: entityID)

    return .init(id: entityID, entity: consume result)

  }

  func yieldContinuously(_ input: Input, storage: Void) -> Verge.ContinuousResult<Output> {

    guard let previous = input.previous else {
      return .new(yield(input, storage: storage))
    }

    if NormalizedStorageComparisons<EntityStorage>.StorageComparison()(selector.storage(source: input.primitive), selector.storage(source: previous.primitive)) {
      return .noUpdates
    }

    if NormalizedStorageComparisons<EntityStorage>.TableComparison<_TableSelector.Table>()(selector.table(source: input.primitive), selector.table(source: previous.primitive)) {
      return .noUpdates
    }

    return .new(yield(input, storage: storage))

  }

}

private struct NonNullSingleEntityPipeline<
  _StorageSelector: StorageSelector,
  _TableSelector: TableSelector
>: PipelineType
where _StorageSelector.Storage == _TableSelector.Storage {

  typealias Entity = _TableSelector.Table.Entity
  typealias Input = Changes<_StorageSelector.Source>
  typealias EntityStorage = _StorageSelector.Storage
  typealias Output = NonNullEntityWrapper<Entity>

  private let selector: AbsoluteTableSelector<_StorageSelector, _TableSelector>
  private let entityID: Entity.EntityID

  private let latestValue: Entity
  private let lock: NSLock = .init()

  init(
    initialEntity: Entity,
    selector: consuming AbsoluteTableSelector<_StorageSelector, _TableSelector>
  ) {

    self.entityID = initialEntity.entityID
    self.latestValue = initialEntity
    self.selector = selector

  }

  func yield(_ input: consuming Input, storage: Void) -> Output {

    let result = selector.table(source: input.primitive)
      .find(by: entityID)

    if let result {
      return .init(entity: result, isFallBack: false)
    } else {
      return .init(entity: latestValue, isFallBack: true)
    }

  }

  func yieldContinuously(_ input: Input, storage: Void) -> Verge.ContinuousResult<Output> {

    guard let previous = input.previous else {
      return .new(yield(input, storage: storage))
    }

    if NormalizedStorageComparisons<EntityStorage>.StorageComparison()(selector.storage(source: input.primitive), selector.storage(source: previous.primitive)) {
      return .noUpdates
    }

    if NormalizedStorageComparisons<EntityStorage>.TableComparison<_TableSelector.Table>()(selector.table(source: input.primitive), selector.table(source: previous.primitive)) {
      return .noUpdates
    }

    return .new(yield(input, storage: storage))

  }

}
