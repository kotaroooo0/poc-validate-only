package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/elastic/cloud-sdk-go/pkg/api"
	"github.com/elastic/cloud-sdk-go/pkg/api/deploymentapi"
	"github.com/elastic/cloud-sdk-go/pkg/api/deploymentapi/deputil"
	"github.com/elastic/cloud-sdk-go/pkg/auth"
	"github.com/elastic/cloud-sdk-go/pkg/models"
	"github.com/elastic/cloud-sdk-go/pkg/plan"
	"github.com/elastic/cloud-sdk-go/pkg/plan/planutil"
)

func main() {
	ecApiKey := os.Getenv("EC_API_KEY")
	deploymentName := os.Getenv("DEPLOYMENT_NAME")
	deploymentScaler, err := NewDeploymentScaler(ecApiKey, deploymentName)
	if err != nil {
		log.Fatal(err)
	}
	if err := deploymentScaler.UpdateDeploymentSpec(1024, false); err != nil {
		log.Fatal(err)
	}
}

type DeploymentScaler struct {
	ecClient       *api.API
	deploymentId   string
	deploymentName string
}

// 指定されたデプロイメント名で新しいDeploymentScalerを作成
// デプロイメントが見つからない場合はエラーを返却
func NewDeploymentScaler(ecApiKey string, deploymentName string) (*DeploymentScaler, error) {
	ecClient, err := api.NewAPI(api.Config{
		Client:     new(http.Client),
		AuthWriter: auth.APIKey(ecApiKey),
	})
	if err != nil {
		return nil, err
	}

	// デプロイメントの一覧を取得
	res, err := deploymentapi.List(deploymentapi.ListParams{API: ecClient})
	if err != nil {
		return nil, err
	}

	// デプロイメントを検索
	for _, deployment := range res.Deployments {
		if *deployment.Name == deploymentName {
			return &DeploymentScaler{
				ecClient:       ecClient,
				deploymentId:   *deployment.ID,
				deploymentName: deploymentName,
			}, nil
		}
	}
	return nil, fmt.Errorf("deployment not found: %s", deploymentName)
}

// デプロイメントのElasticsearchリソース情報を取得
func (s *DeploymentScaler) GetElasticsearchResourceInfo() (*models.ElasticsearchResourceInfo, error) {
	params := deploymentapi.GetParams{
		API:          s.ecClient,
		DeploymentID: s.deploymentId,
		RefID:        "main-elasticsearch",
		QueryParams: deputil.QueryParams{
			ShowSettings:     true,
			ShowPlans:        true,
			ShowPlanDefaults: true,
		},
	}
	return deploymentapi.GetElasticsearch(params)
}

// Elasticsearchデプロイメントのメモリサイズを更新
// デプロイメントの更新に失敗した場合はエラーを返却
func (s *DeploymentScaler) UpdateDeploymentSpec(memorySize int32, validateOnly bool) error {
	// Elasticsearchリソース情報を取得
	esResource, err := s.GetElasticsearchResourceInfo()
	if err != nil {
		return err
	}

	// Updateのためのリクエストを作成
	topologyId := "hot_content" // hot_content, warm_content, cold_content, frozen_contentの中でhot_contentを対象にする
	resource := "memory"        // memory, storageの中でmemoryを対象にする
	for _, topology := range esResource.Info.PlanInfo.Current.Plan.ClusterTopology {
		if topology.ID == topologyId {
			topology.Size = &models.TopologySize{
				Resource: &resource,
				Value:    &memorySize,
			}
		}
	}

	r := models.DeploymentUpdateRequest{
		PruneOrphans: func() *bool { b := false; return &b }(), // FalseにしておけばKibanaなどの他リソースが削除されない
		Resources: &models.DeploymentUpdateResources{
			Elasticsearch: []*models.ElasticsearchPayload{
				{
					Plan:     esResource.Info.PlanInfo.Current.Plan,
					RefID:    esResource.RefID,
					Region:   esResource.Region,
					Settings: esResource.Info.Settings,
				},
			},
		},
	}

	// Updateリクエストを送信
	_, err = deploymentapi.Update(deploymentapi.UpdateParams{
		DeploymentID: s.deploymentId,
		API:          s.ecClient,
		Request:      &r,
		ValidateOnly: validateOnly,
	})
	if err != nil {
		return err
	}

	// デプロイメント更新を待機
	if err := planutil.Wait(plan.TrackChangeParams{
		API:          s.ecClient,
		DeploymentID: s.deploymentId,
		Config: plan.TrackFrequencyConfig{
			PollFrequency: 10,
			MaxRetries:    10,
		},
	}); err != nil {
		return err
	}
	return nil
}
