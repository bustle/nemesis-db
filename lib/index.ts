import { GraphNodeLoader } from './node'
import { GraphNodeInterfaceLoader } from './node-interface'
import { GraphEdgeLoader } from './edge'
import { LabeledEdgeLoader } from './labeled-edge'
import { buildSchema, GraphDBSchema, NodeDefs, InterfaceDefs, EdgeDefs } from './schema'
import { RedisLoader } from 'redis-loader'
import { GraphDB } from 'graphdb/types'

export class GraphLoader {
  // tslint:disable-next-line:variable-name
  readonly _redis: RedisLoader
  readonly edge: GraphEdgeLoader
  readonly labeledEdge: LabeledEdgeLoader
  readonly schema: GraphDBSchema

  // There must be a better way
  readonly AdClip: GraphNodeLoader<GraphDB.AdClip, GraphDB.CreateAdClip>
  readonly ArticlePost: GraphNodeLoader<GraphDB.ArticlePost, GraphDB.CreateArticlePost>
  readonly SceneZone: GraphNodeLoader<GraphDB.SceneZone, GraphDB.CreateSceneZone>
  readonly CarouselZone: GraphNodeLoader<GraphDB.CarouselZone, GraphDB.CreateCarouselZone>
  readonly Category: GraphNodeLoader<GraphDB.Category, GraphDB.CreateCategory>
  readonly CirculationOrder: GraphNodeLoader<GraphDB.CirculationOrder, GraphDB.CreateCirculationOrder>
  readonly Clip: GraphNodeInterfaceLoader<GraphDB.Clip>
  readonly Content: GraphNodeInterfaceLoader<GraphDB.Content>
  readonly ContentFeed: GraphNodeLoader<GraphDB.ContentFeed, GraphDB.CreateContentFeed>
  readonly EmailPage: GraphNodeLoader<GraphDB.EmailPage, GraphDB.CreateEmailPage>
  readonly FeatureZone: GraphNodeLoader<GraphDB.FeatureZone, GraphDB.CreateFeatureZone>
  readonly Feed: GraphNodeInterfaceLoader<GraphDB.Feed>
  readonly FeedZone: GraphNodeLoader<GraphDB.FeedZone, GraphDB.CreateFeedZone>
  readonly GridZone: GraphNodeLoader<GraphDB.GridZone, GraphDB.CreateGridZone>
  readonly Group: GraphNodeLoader<GraphDB.Group, GraphDB.CreateGroup>
  readonly HeroZone: GraphNodeLoader<GraphDB.HeroZone, GraphDB.CreateHeroZone>
  readonly Image: GraphNodeLoader<GraphDB.Image, GraphDB.CreateImage>
  readonly ImageClip: GraphNodeLoader<GraphDB.ImageClip, GraphDB.CreateImageClip>
  readonly ListiclePost: GraphNodeLoader<GraphDB.ListiclePost, GraphDB.CreateListiclePost>
  readonly ListZone: GraphNodeLoader<GraphDB.ListZone, GraphDB.CreateListZone>
  readonly MapZone: GraphNodeLoader<GraphDB.MapZone, GraphDB.CreateMapZone>
  readonly MobiledocClip: GraphNodeLoader<GraphDB.MobiledocClip, GraphDB.CreateMobiledocClip>
  readonly MostWantedPage: GraphNodeLoader<GraphDB.MostWantedPage, GraphDB.CreateMostWantedPage>
  readonly Node: GraphNodeInterfaceLoader<GraphDB.Node>
  readonly Oembed: GraphNodeLoader<GraphDB.Oembed, GraphDB.CreateOembed>
  readonly OembedClip: GraphNodeLoader<GraphDB.OembedClip, GraphDB.CreateOembedClip>
  readonly Page: GraphNodeInterfaceLoader<GraphDB.Page>
  readonly PageClip: GraphNodeLoader<GraphDB.PageClip, GraphDB.CreatePageClip>
  readonly Post: GraphNodeInterfaceLoader<GraphDB.Post>
  readonly PostClip: GraphNodeLoader<GraphDB.PostClip, GraphDB.CreatePostClip>
  readonly PostRevision: GraphNodeLoader<GraphDB.PostRevision, GraphDB.CreatePostRevision>
  readonly PostsByAuthorsFeed: GraphNodeLoader<GraphDB.PostsByAuthorsFeed, GraphDB.CreatePostsByAuthorsFeed>
  readonly PostsByTagsFeed: GraphNodeLoader<GraphDB.PostsByTagsFeed, GraphDB.CreatePostsByTagsFeed>
  readonly Product: GraphNodeLoader<GraphDB.Product, GraphDB.CreateProduct>
  readonly ProductClip: GraphNodeLoader<GraphDB.ProductClip, GraphDB.CreateProductClip>
  readonly ProfilePage: GraphNodeLoader<GraphDB.ProfilePage, GraphDB.CreateProfilePage>
  readonly RevealZone: GraphNodeLoader<GraphDB.RevealZone, GraphDB.CreateRevealZone>
  readonly Root: GraphNodeLoader<GraphDB.Root, GraphDB.CreateRoot>
  readonly Scene: GraphNodeInterfaceLoader<GraphDB.Scene>
  readonly ScratchScene: GraphNodeLoader<GraphDB.ScratchScene, GraphDB.CreateScratchScene>
  readonly Site: GraphNodeLoader<GraphDB.Site, GraphDB.CreateSite>
  readonly SlideshowPost: GraphNodeLoader<GraphDB.SlideshowPost, GraphDB.CreateSlideshowPost>
  readonly StoryPage: GraphNodeLoader<GraphDB.StoryPage, GraphDB.CreateStoryPage>
  readonly Tag: GraphNodeLoader<GraphDB.Tag, GraphDB.CreateTag>
  readonly TopicPage: GraphNodeLoader<GraphDB.TopicPage, GraphDB.CreateTopicPage>
  readonly User: GraphNodeLoader<GraphDB.User, GraphDB.CreateUser>
  readonly Vertical: GraphNodeLoader<GraphDB.Vertical, GraphDB.CreateVertical>
  readonly Zone: GraphNodeInterfaceLoader<GraphDB.Zone>

  constructor({
    redis,
    nodeDefs,
    interfaceDefs,
    edgeDefs,
  }: {
    readonly edgeDefs: EdgeDefs
    readonly interfaceDefs: InterfaceDefs
    readonly nodeDefs: NodeDefs
    readonly redis: RedisLoader
  }) {
    this._redis = redis
    const schema = buildSchema({ nodeDefs, interfaceDefs, edgeDefs })
    this.schema = schema
    this.edge = new GraphEdgeLoader({
      schema,
      redis,
    })

    this.labeledEdge = new LabeledEdgeLoader({
      schema,
      redis,
    })

    for (const typeDef of Object.values(schema.nodeDefs)) {
      ;(this as any)[typeDef.name] = new GraphNodeLoader({
        redis,
        typeDef,
        schema,
      })
    }

    for (const typeDef of Object.values(schema.interfaceDefs)) {
      ;(this as any)[typeDef.name] = new GraphNodeInterfaceLoader({
        redis,
        typeDef,
        schema,
      })
    }
  }

  resetStats(): void {
    if (this._redis instanceof RedisLoader) {
      return this._redis.resetStats()
    }
  }
}
